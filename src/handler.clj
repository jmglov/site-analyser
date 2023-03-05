(ns handler
  (:require [cheshire.core :as json]
            [logs]
            [util :refer [log error]]))

(def config
  {:region (System/getenv "AWS_REGION")
   :s3-bucket (System/getenv "S3_BUCKET")
   :s3-prefix (System/getenv "S3_PREFIX")
   :cloudfront-dist-id (System/getenv "CLOUDFRONT_DIST_ID")
   :log-type (or (System/getenv "LOG_TYPE") :cloudfront)})

(log "Lambda starting" {:config config})

(def logs-client (logs/mk-client config))

(defn handle-request
  ([]
   (handle-request {} {}))
  ([event]
   (handle-request event {}))
  ([event _context]
   (log "Invoked with event" {:event event})
   (try
     (let [{:keys [log-type]} config
           params (get event "queryStringParameters" event)
           date-str (event "date")
           date (util/get-date date-str)
           body (logs/get-log-entries logs-client date log-type)]
       (log "Successfully parsed logs" body)
       {"statusCode" 200
        "body" (json/encode body)})
     (catch Exception e
       (log (ex-message e) (ex-data e))
       {"statusCode" 400
        "body" (ex-message e)}))))
