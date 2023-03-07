(ns util
  (:require [clojure.java.io :as io]))

(defmacro ->map [& ks]
  (assert (every? symbol? ks))
  (zipmap (map keyword ks)
          ks))

(defn ->int [s]
  (Integer/parseUnsignedInt s))

(defn log [msg data]
  (prn {:msg msg
        :data data
        :timestamp (str (Instant/now))}))

(defn error [msg data]
  (log msg data)
  (throw (ex-info msg data)))

(defn lazy-concat [colls]
  (lazy-seq
   (when-first [c colls]
     (lazy-cat c (lazy-concat (rest colls))))))

(defn parse-int [s]
  (when s
    (try
      (Integer/parseInt s)
      (catch Exception _
        (error (format "Failed to parse integer: %s" s) {:string s})))))

(defn read-edn [f]
  (with-open [r (io/reader f)]
    (read (java.io.PushbackReader. r))))
