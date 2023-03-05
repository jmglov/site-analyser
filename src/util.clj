(ns util
  (:require [clojure.java.io :as io])
  (:import (java.time Instant
                      LocalDate)))

(defn log [msg data]
  (prn {:msg msg
        :data data
        :timestamp (str (Instant/now))}))

(defn error [msg data]
  (log msg data)
  (throw (ex-info msg data)))

(defn get-date [date-str]
  (if date-str
    (try
      (LocalDate/parse date-str)
      (catch Exception _
        (error (format "Invalid date: %s" date-str) {:date date-str})))
    (LocalDate/now)))

(defn parse-int [s]
  (when s
    (try
      (Integer/parseInt s)
      (catch Exception _
        (error (format "Failed to parse integer: %s" s) {:string s})))))

(defn read-edn [f]
  (with-open [r (io/reader f)]
    (read (java.io.PushbackReader. r))))
