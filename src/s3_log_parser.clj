(ns s3-log-parser
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [com.grzm.awyeah.client.api :as aws])
  (:import (java.time Instant
                      LocalDate)))

(defn log [msg data]
  (prn {:msg msg
        :data data
        :timestamp (str (Instant/now))}))

(def config
  {:region (System/getenv "AWS_REGION")
   :s3-bucket (System/getenv "S3_BUCKET")
   :s3-prefix (System/getenv "S3_PREFIX")})

(log "Lambda starting" {:config config})

(def s3 (aws/client {:api :s3, :region (:region config)}))

(defn get-date [date-str]
  (if date-str
    (try
      (LocalDate/parse date-str)
      (catch Exception _
        (throw (Exception. (format "Invalid date: %s" date-str)))))
    (LocalDate/now)))

(defn list-logs [date]
  (let [{:keys [region s3-bucket s3-prefix]} config
        request {:Bucket s3-bucket
                 :Prefix (format "%s%s" s3-prefix date)
                 :MaxKeys 2}
        response (aws/invoke s3 {:op :ListObjectsV2
                                 :request request})]
    (log "Listing log files" request)
    (log "Response" response)
    (->> response
         :Contents
         (map :Key))))

(defn handler [event context]
  (log "Invoked with event" {:event event})
  (try
    (let [date (get-date (get-in event ["queryStringParameters" "date"]))
          logs (list-logs date)]
      {"statusCode" 200
       "body" (json/encode {:logs logs})})
    (catch Exception e
      (println e)
      {"statusCode" 400
       "body" (.getMessage e)})))
