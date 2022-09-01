(ns parser
  (:require [clojure.string :as str]))

(def log-fields
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

(def log-keys (map first log-fields))
(def log-regex (->> log-fields
                    (map second)
                    (str/join " ")
                    (format "^%s(.*)$")
                    re-pattern))

(defn parse-line [log-line]
  (->> log-line
       (re-seq log-regex)
       first
       (drop 1)
       (interleave log-keys)
       (partition-all 2)
       (remove (fn [[k v]] (= "-" v)))
       (map vec)
       (into {})))
