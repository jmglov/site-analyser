(ns logs
  (:require [clojure.java.io :as io]
            [babashka.fs :as fs]
            [com.grzm.awyeah.client.api :as aws]
            [parser]
            [requests]
            [time]
            [util :refer [lazy-concat log error] :as util])
  (:import (java.util.zip GZIPInputStream)))

(defn handle-error [{err :Error :as resp}]
  (if err
    (error (:Message err) err)
    resp))

;; Extra config:
;; - :limit N  if set, limit to one N logfiles
(defn mk-client [{:keys [region] :as config}]
  (assoc config :s3-client (aws/client {:api :s3, :region region})))

(defn mk-s3-req
  ([s3-bucket prefix s3-page-size]
   (mk-s3-req s3-bucket prefix s3-page-size nil))
  ([s3-bucket prefix s3-page-size continuation-token]
   (merge {:Bucket s3-bucket
           :Prefix prefix}
          (when s3-page-size
            {:MaxKeys s3-page-size})
          (when continuation-token
            {:ContinuationToken continuation-token}))))

(defn get-s3-page [{:keys [s3-client s3-bucket s3-page-size]}
                   prefix
                   {continuation-token :NextContinuationToken
                    truncated? :IsTruncated
                    page-num :page-num
                    :as prev}]
  (when prev (log "Got page" (dissoc prev :Contents)))
  (let [page-num (inc (or page-num 0))
        done? (false? truncated?)
        request (mk-s3-req s3-bucket prefix s3-page-size continuation-token)
        response (when-not done?
                   (log (format "Requesting page %d" page-num) request)
                   (-> (aws/invoke s3-client {:op :ListObjectsV2
                                              :request request})
                       (assoc :page-num page-num)))]
    response))

(defn list-objects [{:keys [limit] :as logs-client} prefix]
  (let [apply-limit (if limit (partial take limit) identity)]
    (->> (iteration (partial get-s3-page logs-client prefix)
                    :vf :Contents)
         lazy-concat
         apply-limit
         (map :Key))))

(defn list-cloudfront-logs
  ([client]
   (list-cloudfront-logs client nil))
  ([{:keys [s3-prefix cloudfront-dist-id] :as client} date]
   (let [prefix (if date
                  (format "%s%s.%s-" s3-prefix cloudfront-dist-id date)
                  (format "%s%s." s3-prefix cloudfront-dist-id))]
     (list-objects client prefix))))

(defn list-s3-logs [{:keys [s3-prefix] :as client} date]
  (list-objects client (format "%s%s" s3-prefix date)))

(defn list-logs [client date log-type]
  (let [list-fn (case log-type
                  :cloudfront list-cloudfront-logs
                  :s3 list-s3-logs
                  (throw (ex-info (format "Invalid log type: %s" log-type)
                                  {:logs/error :invalid-log-type
                                   :log-type log-type})))]
    (list-fn client date)))

(defn get-lines [{content-type :ContentType
                  body :Body}]
  (let [content-handler
        (condp = content-type
          "application/x-gzip" #(GZIPInputStream. %)
          "text/plain" identity
          (throw (ex-info (format "Unexpected content type: %s" content-type)
                          {:logs/error :unexpected-content-type
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

(defn summarise-cloudfront-entry
  [{:keys [date time c-ip cs-uri-stem cs-user-agent referer x-edge-request-id]}]
  {:date date
   :time time
   :ip c-ip
   :path cs-uri-stem
   :referer referer
   :request-id x-edge-request-id
   :user-agent cs-user-agent})

(defn summarise-s3-entry
  [{:keys [time remote-ip request-id request-uri referer user-agent]}]
  (let [{:keys [date time]} (time/parse-datetime "dd/MMM/yyyy:HH:mm:s Z" time)
        path (-> (re-seq #"^\w+ ([^? ]+)[? ].+$" request-uri) first second)]
    {:date date
     :time time
     :ip remote-ip
     :path path
     :referer referer
     :request-id request-id
     :user-agent user-agent}))

(defn get-log-entries
  ([client date]
   (get-log-entries client date :cloudfront {}))
  ([client date log-type]
   (let [summarise (case log-type
                     :cloudfront summarise-cloudfront-entry
                     :s3 summarise-s3-entry
                     (throw (ex-info (format "Invalid log type: %s" log-type)
                                     {:logs/error :invalid-log-type
                                      :log-type log-type})))
         logs (list-logs client date log-type)
         entries (->> logs
                      (mapcat (partial get-log-lines client))
                      (parser/parse-lines log-type)
                      (map summarise))]
     {:date (str date), :log-type log-type, :logs logs, :entries entries})))

(defn write-entries! [dir {:keys [date] :as entries}]
  (spit (fs/file dir (format "%s.edn" date))
        (-> entries
            (update :logs vec)
            (update :entries vec))))

(defn read-entries [dir date]
  (with-open [r (io/reader (fs/file dir (format "%s.edn" date)))]
    (read (java.io.PushbackReader. r))))
