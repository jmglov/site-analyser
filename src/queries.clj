(ns queries
  (:require [babashka.fs :as fs]
            [logs]
            [util :refer [log]])
  (:import (java.time LocalDate)))

(defn dates-in [year month]
  (let [start-date (LocalDate/of year month 1)]
    (->> (iterate #(.plusDays % 1) start-date)
         (take-while #(= month (.getMonthValue %)))
         (map str))))

(defn read-entries [date]
  (log "Reading entries" {:date date})
  (try
    (->> (logs/read-entries base-dir date)
         :entries)
    (catch Exception _
      (log "Failed to open file" {:date date}))))

(defn blog-post? [{:keys [cs-uri-stem]}]
  (re-matches #"^/blog/[^/]+[.]html$" cs-uri-stem))

(defn sort-posts [year month]
  (let [data (->> (dates-in year month)
                  (sequence (comp (mapcat #(read-entries %))
                                  (filter blog-post?)))
                  (group-by :cs-uri-stem)
                  (map (fn [[uri entries]] [uri (count entries)]))
                  (sort-by second)
                  reverse
                  vec)
        out-filename (format "by-post.%d-%02d.edn" year month)]
    (log "Writing posts" {:out-filename out-filename
                          :data data})
    (spit (fs/file base-dir out-filename) data)))
