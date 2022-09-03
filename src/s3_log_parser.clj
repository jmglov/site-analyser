(ns s3-log-parser
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core :as json]
            [com.grzm.awyeah.client.api :as aws]
            [parser])
  (:import (java.time Instant
                      LocalDate)))

(defn log [msg data]
  (prn {:msg msg
        :data data
        :timestamp (str (Instant/now))}))

(defn error [msg data]
  (log msg data)
  (throw (ex-info msg data)))

(def config
  {:region (System/getenv "AWS_REGION")
   :s3-bucket (System/getenv "S3_BUCKET")
   :s3-prefix (System/getenv "S3_PREFIX")})

(defn handle-error [{err :Error :as resp}]
  (if err
    (error (:Message err) err)
    resp))

(log "Lambda starting" {:config config})

(def s3 (aws/client {:api :s3, :region (:region config)}))

(defn get-date [date-str]
  (if date-str
    (try
      (LocalDate/parse date-str)
      (catch Exception _
        (error (format "Invalid date: %s" date-str) {:date date-str})))
    (LocalDate/now)))

(defn list-logs [date]
  (let [{:keys [region s3-bucket s3-prefix]} config
        request {:Bucket s3-bucket
                 :Prefix (format "%s%s" s3-prefix date)}
        response (aws/invoke s3 {:op :ListObjectsV2
                                 :request request})]
    (log "Listing log files" request)
    (log "Response" response)
    (->> response
         handle-error
         :Contents
         (map :Key))))

(defn get-log-lines [s3-key]
  (->> (aws/invoke s3 {:op :GetObject
                       :request {:Bucket (:s3-bucket config)
                                 :Key s3-key}})
       handle-error
       :Body
       io/reader
       line-seq))

(defn handler
  ([event]
   (handler event {}))
  ([event _context]
   (log "Invoked with event" {:event event})
   (try
     (let [date (get-date (get-in event ["queryStringParameters" "date"]))
           logs (list-logs date)
           log-entries (->> logs
                            (mapcat get-log-lines)
                            (map parser/parse-line))
           body {:date (str date), :logs logs, :entries log-entries}]
       (log "Successfully parsed logs" body)
       {"statusCode" 200
        "body" (json/encode body)})
     (catch Exception e
       (log (ex-message e) (ex-data e))
       {"statusCode" 400
        "body" (ex-message e)}))))
