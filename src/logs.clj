(ns logs
  (:require [clojure.java.io :as io]
            [babashka.fs :as fs]
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
     {:date (str date), :log-type log-type, :logs logs, :entries entries})))

(defn write-entries! [dir {:keys [date] :as entries}]
  (spit (fs/file dir (format "%s.edn" date))
        (-> entries
            (update :logs vec)
            (update :entries vec))))

(defn read-entries [dir date]
  (with-open [r (io/reader (fs/file dir (format "%s.edn" date)))]
    (read (java.io.PushbackReader. r))))
