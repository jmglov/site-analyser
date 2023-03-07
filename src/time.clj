(ns time
  (:require [util :refer [error]])
  (:import (java.time Instant
                      LocalDate
                      ZonedDateTime)
           (java.time.format DateTimeFormatter)))

(defn get-date [date-str]
  (if date-str
    (try
      (LocalDate/parse date-str)
      (catch Exception _
        (error (format "Invalid date: %s" date-str) {:date date-str})))
    (LocalDate/now)))

(defn parse-datetime [format-str datetime-str]
  (let [formatter (DateTimeFormatter/ofPattern format-str)
        dt (ZonedDateTime/parse datetime-str formatter)]
    {:date (.format dt (DateTimeFormatter/ISO_LOCAL_DATE))
     :time (.format dt (DateTimeFormatter/ISO_TIME))}))
