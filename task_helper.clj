(ns task-helper
  (:require [babashka.fs :as fs]
            [clojure.string :as string]
            [pod.babashka.aws :as aws]))

(def defaults
  {:aws-region {:doc "AWS region"
                :default (or (System/getenv "AWS_DEFAULT_REGION") "eu-west-1")}
   :bb-arch {:doc "Architecture to target"
             :default "arm64"
             :values ["amd64" "arm64"]}
   :lambda-handler {:doc "Lambda request handler function, fully qualified"
                    :default "s3-log-parser/handler"}
   :lambda-name {:doc "Lambda function name"
                 :default "s3-log-parser"}
   :lambda-role {:doc "IAM role name"
                 :default "s3-log-parser-lambda"}
   :deps-layer {:doc "Name of deps layer (uses latest version unless specified as name:version)"
                :default "s3-log-parser-deps"}
   :pods-layer {:doc "Name of pods layer (uses latest version unless specified as name:version)"
                :default "s3-log-parser-pods"}
   :runtime-layer {:doc "Name of custom runtime layer (uses latest version unless specified as name:version)"
                   :default "blambda"}
   :target-dir {:doc "Build output directory"
                :default "target"}
   :work-dir {:doc "Working directory"
              :default ".work"}})

(defn help []
  (println (str
            "Available arguments:\n"
            (->> defaults
                 (map (fn [[k v]]
                        (let [arg (str "--" (name k))
                              {:keys [doc default values]} v
                              valid-values (when values
                                             (str " (valid values: "
                                                  (string/join ", " values)
                                                  ")"))]
                          (str "\t" arg "\t" doc " (default: " default ")" valid-values))))
                 (string/join "\n")))))

(defn error [msg]
  (println msg)
  (println)
  (help)
  (System/exit 1))

(defn parse-args []
  (when-not (even? (count *command-line-args*))
    (error "Error: odd number of command-line args"))
  (let [default-args (->> defaults
                          (map (fn [[k {:keys [default]}]] [k default]))
                          (into {}))]
    (->> *command-line-args*
         (partition 2)
         (map (fn [[k v]]
                (let [arg (keyword (string/replace-first k "--" ""))]
                  (when-not (contains? default-args arg)
                    (error (str "Error: invalid argument: " k)))
                  [arg v])))
         (into {})
         (merge default-args))))

(defn target-file [target-dir filename]
  (format "%s/%s" (-> (fs/file target-dir) .getAbsolutePath) filename))

(defn latest-layer-version [lambda layer-name]
  (if (string/includes? layer-name ":")
    layer-name
    (let [latest-version (->> (aws/invoke lambda {:op :ListLayerVersions
                                                  :request {:LayerName layer-name}})
                              :LayerVersions
                              (sort-by :Version)
                              last
                              :Version)]
      (format "%s:%s" layer-name latest-version))))

(defn create-lambda [lambda
                     {:keys [aws-region bb-arch
                             lambda-handler lambda-name lambda-role
                             pods-layer runtime-layer zipfile]}]
  (let [lambda-arch (if (= "amd64" bb-arch) "x86_64" "arm64")
        runtime (if (= "amd64" bb-arch) "provided" "provided.al2")
        sts (aws/client {:api :sts
                         :region aws-region})
        account-id (-> (aws/invoke sts {:op :GetCallerIdentity}) :Account)
        layer-arns (->> [runtime-layer pods-layer]
                        (map #(format "arn:aws:lambda:%s:%s:layer:%s"
                                      aws-region account-id
                                      (latest-layer-version lambda %))))
        role-arn (format "arn:aws:iam::%s:role/%s"
                         account-id lambda-role)
        req {:FunctionName lambda-name
             :Runtime runtime
             :Role role-arn
             :Code {:ZipFile zipfile}
             :Handler lambda-handler
             :Layers layer-arns
             :Architectures [lambda-arch]}
        _ (println "Creating lambda:" (pr-str req))
        res (aws/invoke lambda {:op :CreateFunction
                                :request req})]
    (when (contains? res :cognitect.anomalies/category)
      (println "Error:" (pr-str res)))))

(defn update-lambda [lambda runtime-layer zipfile])

(defn create-or-update-lambda [{:keys [aws-region bb-arch
                                       lambda-handler lambda-name lambda-role
                                       runtime-layer zipfile]
                                :as args}]
  (let [lambda (aws/client {:api :lambda
                            :region aws-region})
        _ (println "Checking to see if lambda exists:" lambda-name)
        lambda-exists? (-> (aws/invoke lambda {:op :GetFunction
                                               :request {:FunctionName lambda-name}})
                           (contains? :Configuration))]
    (if lambda-exists?
      (update-lambda lambda args)
      (create-lambda lambda args))))

(defn deploy-layer [{:keys [layer-name layer-filename runtimes architectures]}]
  (let [{:keys [aws-region target-dir]} (parse-args)
        layer-filename (target-file target-dir layer-filename)
        client (aws/client {:api :lambda
                            :region aws-region})
        zipfile (fs/read-all-bytes layer-filename)
        request (merge {:LayerName layer-name
                        :Content {:ZipFile zipfile}}
                       (when runtimes {:CompatibleRuntimes runtimes})
                       (when architectures {:CompatibleArchitectures architectures}))
        _ (println "Publishing layer version for layer" layer-name)
        res (aws/invoke client {:op :PublishLayerVersion
                                :request request})]
    (if (:cognitect.anomalies/category res)
      (prn "Error:" res)
      (println "Published layer" (:LayerVersionArn res)))))
