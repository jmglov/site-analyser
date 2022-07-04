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
   :layer-name {:doc "Name and version of custom runtime layer"
                :default "blambda:1"}
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

(defn lambda-zipfile [target-dir]
  (str (-> (fs/file target-dir) .getAbsolutePath)
       "/function.zip"))

(defn create-lambda [lambda aws-region bb-arch
                     lambda-handler lambda-name lambda-role
                     layer-name zipfile]
  (let [lambda-arch (if (= "amd64" bb-arch) "x86_64" "arm64")
        runtime (if (= "amd64" bb-arch) "provided" "provided.al2")
        sts (aws/client {:api :sts
                         :region aws-region})
        account-id (-> (aws/invoke sts {:op :GetCallerIdentity}) :Account)
        layer-arn (format "arn:aws:lambda:%s:%s:layer:%s"
                          aws-region account-id layer-name)
        role-arn (format "arn:aws:iam::%s:role/%s"
                         account-id lambda-role)
        req {:FunctionName lambda-name
             :Runtime runtime
             :Role role-arn
             :Code {:ZipFile zipfile}
             :Handler lambda-handler
             :Layers [layer-arn]
             :Architectures [lambda-arch]}
        _ (println "Creating lambda:" (pr-str req))
        res (aws/invoke lambda {:op :CreateFunction
                                :request req})]
    (when (contains? res :cognitect.anomalies/category)
      (println "Error:" (pr-str res)))))

(defn update-lambda [lambda layer-name zipfile])

(defn create-or-update-lambda [aws-region bb-arch
                               lambda-handler lambda-name lambda-role
                               layer-name zipfile]
  (let [lambda (aws/client {:api :lambda
                            :region aws-region})
        _ (println "Checking to see if lambda exists:" lambda-name)
        lambda-exists? (-> (aws/invoke lambda {:op :GetFunction
                                               :request {:FunctionName lambda-name}})
                           (contains? :Configuration))]
    (if lambda-exists?
      (update-lambda lambda lambda-name zipfile)
      (create-lambda lambda aws-region bb-arch
                     lambda-handler lambda-name lambda-role
                     layer-name zipfile))))
