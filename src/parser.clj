(ns parser
  (:require [clojure.string :as str]))

(def s3-fields
  (->> [:bucket-owner "(\\S+)"
        :bucket "(\\S+)"
        :time "\\[([^]]+)\\]"
        :remote-ip "(\\S+)"
        :requester "(\\S+)"
        :request-id "(\\S+)"
        :operation "(\\S+)"
        :key "(\\S+)"
        :request-uri "\"([^\"]+)\""
        :http-status "(\\S+)"
        :error-code "(\\S+)"
        :bytes-sent "(\\S+)"
        :object-size "(\\S+)"
        :total-time "(\\S+)"
        :turn-around-time "(\\S+)"
        :referer "\"([^\"]+)\""
        :user-agent "\"([^\"]+)\""
        :version-id "(\\S+)"
        :host-id "(\\S+)"
        :signature-version "(\\S+)"
        :cipher-suite "(\\S+)"
        :authentication-type "(\\S+)"
        :host-header "(\\S+)"
        :tls-version "(\\S+)"
        :access-point-arn "(\\S+)"]
       (partition-all 2)))

(def s3-keys (map first s3-fields))
(def s3-regex (->> s3-fields
                   (map second)
                   (str/join " ")
                   (format "^%s(.*)$")
                   re-pattern))

(def cloudfront-keys [:date
                      :time
                      :x-edge-location
                      :sc-bytes
                      :c-ip
                      :cs-method
                      :cs-host
                      :cs-uri-stem
                      :sc-status
                      :cs-referer
                      :cs-user-agent
                      :cs-uri-query
                      :cs-cookie
                      :x-edge-result-type
                      :x-edge-request-id
                      :x-host-header
                      :cs-protocol
                      :cs-bytes
                      :time-taken
                      :x-forwarded-for
                      :ssl-protocol
                      :ssl-cipher
                      :x-edge-response-result-type
                      :cs-protocol-version
                      :fle-status
                      :fle-encrypted-fields
                      :c-port
                      :time-to-first-byte
                      :x-edge-detailed-result-type
                      :sc-content-type
                      :sc-content-len
                      :sc-range-start
                      :sc-range-end])

(defn parse-cloudfront-line [log-line]
  (zipmap cloudfront-keys
          (->> (str/split log-line #"\t")
               (map #(when-not (= "-" %) %)))))

(defn parse-s3-line [log-line]
  (->> log-line
       (re-seq s3-regex)
       first
       (drop 1)
       (map #(if (= "-" %) nil %))
       (zipmap s3-keys)))

(defn parse-lines [log-type {:keys [log-file lines]}]
  {:log-file log-file
   :entries (case log-type
              :cloudfront (->> lines
                               (sequence (comp (remove #(str/starts-with? % "#"))
                                               (map parse-cloudfront-line))))
              :s3 (map parse-s3-line lines)
              (throw (ex-info (format "Invalid log type: %s" log-type)
                              {:s3-log-parser/error :invalid-log-type
                               :log-type log-type})))})
