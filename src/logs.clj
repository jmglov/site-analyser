(ns logs
  (:require [clojure.java.io :as io]
            [com.grzm.awyeah.client.api :as aws]
            [parser]
            [util :refer [log error]])
  (:import (java.util.zip GZIPInputStream)))

(defn handle-error [{err :Error :as resp}]
  (if err
    (error (:Message err) err)
    resp))

(defn mk-client [{:keys [region] :as config}]
  (assoc config :s3-client (aws/client {:api :s3, :region region})))

(defn list-objects
  ([client prefix]
   (list-objects client prefix {}))
  ([{:keys [s3-client s3-bucket]} prefix {:keys [limit]}]
   (let [request (merge {:Bucket s3-bucket
                         :Prefix prefix}
                        (when limit
                          {:MaxKeys limit}))
         response (aws/invoke s3-client {:op :ListObjectsV2
                                         :request request})]
     (log "Listing objects" request)
     (log "Response" response)
     (->> response
          handle-error
          :Contents
          (map :Key)))))

(defn list-cloudfront-logs
  ([client]
   (list-cloudfront-logs client nil {}))
  ([client date]
   (list-cloudfront-logs client date {}))
  ([{:keys [s3-prefix cloudfront-dist-id] :as client} date opts]
   (let [prefix (if date
                  (format "%s%s.%s-" s3-prefix cloudfront-dist-id date)
                  (format "%s%s." s3-prefix cloudfront-dist-id))]
     (list-objects client prefix opts))))

(defn list-s3-logs
  ([client date]
   (list-s3-logs client date {}))
  ([{:keys [s3-prefix] :as client} date opts]
   (list-objects client (format "%s%s" s3-prefix date) opts)))

(defn list-logs
  ([client date log-type]
   (list-logs client date log-type {}))
  ([client date log-type opts]
   (let [list-fn (case log-type
                   :cloudfront list-cloudfront-logs
                   :s3 list-s3-logs
                   (throw (ex-info (format "Invalid log type: %s" log-type)
                                   {:s3-log-parser/error :invalid-log-type
                                    :log-type log-type})))]
     (list-fn client date opts))))

(defn get-lines [{content-type :ContentType
                  body :Body}]
  (let [content-handler
        (condp = content-type
          "application/x-gzip" #(GZIPInputStream. %)
          "text/plain" identity
          (throw (ex-info (format "Unexpected content type: %s" content-type)
                          {:s3-log-parser/error :unexpected-content-type
                           :content-type content-type})))]
    (->> body
         content-handler
         io/reader
         line-seq)))

(defn get-log-lines [{:keys [s3-client s3-bucket s3-prefix]} s3-key]
  (->> (aws/invoke s3-client {:op :GetObject
                              :request {:Bucket s3-bucket
                                        :Key s3-key}})
       handle-error
       get-lines))

(defn get-log-entries
  ([client date]
   (get-log-entries client date :cloudfront {}))
  ([client date log-type]
   (get-log-entries client date log-type {}))
  ([client date log-type opts]
   (let [logs (list-logs client date log-type opts)
         entries (->> logs
                      (mapcat (partial get-log-lines client))
                      (parser/parse-lines log-type))]
     {:log-type log-type, :logs logs, :entries entries})))

(comment

  (def config {:aws-region "eu-west-1"
               :s3-bucket "logs.jmglov.net"
               :s3-prefix "logs/"
               :cloudfront-dist-id "E1HJS54CQLFQU4"})
  ;; => #<Var@8216104:
  ;;      {:aws-region "eu-west-1",
  ;;       :s3-bucket "logs.jmglov.net",
  ;;       :s3-prefix "logs/",
  ;;       :cloudfront-dist-id "E1HJS54CQLFQU4"}>

  (def logs-client (mk-client config))

  (list-logs logs-client "2022-09-03" :cloudfront {:limit 1})
  ;; => ("logs/E1HJS54CQLFQU4.2022-09-03-00.18a84fe8.gz")

  (list-logs logs-client "2022-09-03" :s3 {:limit 1})
  ;; => ("logs/2022-09-03-00-11-39-EB3F7366DA9271E8")

  (get-log-lines logs-client
                 (first (list-cloudfront-logs logs-client
                                              "2022-09-03"
                                              {:limit 1})))
  ;; => ("#Version: 1.0"
  ;;     "#Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type cs-protocol-version fle-status fle-encrypted-fields c-port time-to-first-byte x-edge-detailed-result-type sc-content-type sc-content-len sc-range-start sc-range-end"
  ;;     "2022-09-03\t00:56:20\tDFW55-C1\t1919\t66.249.73.23\tGET\td3bulohh9org4y.cloudfront.net\t/\t200\t-\tMozilla/5.0%20(compatible;%20Googlebot/2.1;%20+http://www.google.com/bot.html)\t-\t-\tHit\toMQkwemTOCP7Y3Oj4ueAhdjPx1mhtotp20thm33N0BEkoNo8_xdSKg==\twww.jmglov.net\thttps\t293\t0.033\t-\tTLSv1.3\tTLS_AES_128_GCM_SHA256\tHit\tHTTP/1.1\t-\t-\t43350\t0.033\tHit\ttext/html\t-\t-\t-"
  ;;     "2022-09-03\t00:56:20\tDFW55-C1\t580\t66.249.73.28\tGET\td3bulohh9org4y.cloudfront.net\t/\t301\t-\tMozilla/5.0%20(compatible;%20Googlebot/2.1;%20+http://www.google.com/bot.html)\t-\t-\tRedirect\tCPL5Efkr9IFJcSzwkioyS8_wl_OlMatuxKWl7rackaFjdYXi2JZs2w==\twww.jmglov.net\thttp\t293\t0.000\t-\t-\t-\tRedirect\tHTTP/1.1\t-\t-\t47050\t0.000\tRedirect\ttext/html\t183\t-\t-")

  (get-log-lines logs-client
                 (first (list-s3-logs logs-client
                                      "2022-09-03"
                                      {:limit 1})))
  ;; => ("022d83ad6361dec3c93757e75c1c3a7982532ffdbf3bf87976490873591e2188 jmglov.net [02/Sep/2022:23:11:28 +0000] 64.252.70.219 - 8K9166G4KGR6EACQ WEBSITE.GET.OBJECT blog/assets/2022-09-02-code1.png \"GET /blog/assets/2022-09-02-code1.png HTTP/1.1\" 304 - - 151328 21 - \"-\" \"Amazon CloudFront\" - q5O1Lp5Toi1BJoene+95P+3xwSGVgqpyb39MP9rLh+ll0v1RoH+HuliLcAV7LmVCBLd73XYPa3g= - - - jmglov.net.s3-website-eu-west-1.amazonaws.com - -")

  (get-log-entries logs-client "2022-09-03" :cloudfront {:limit 2})

  (get-log-entries logs-client "2022-09-03" :s3 {:limit 2})

  (let [date "2022-09-03"]
    (spit (format "/tmp/cf-logs.%s.edn" date)
          (-> (get-log-entries logs-client date :cloudfront {:limit 2})
              (update :logs vec)
              (update :entries vec))))
  ;; => nil

  (let [date "2022-09-03"]
    (-> (slurp (format "/tmp/cf-logs.%s.edn" date))
        read-string))

  )
