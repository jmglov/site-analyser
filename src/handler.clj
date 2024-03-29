(ns handler
  (:require [cheshire.core :as json]
            [selmer.parser :as selmer]
            [sci.core :as sci]
            [favicon]
            [page-views]
            [logs]
            [time]
            [util :refer [->map log error]])
  (:import (java.time LocalDate)
           (java.util UUID)))

(defn get-env
  ([k]
   (or (System/getenv k)
       (let [msg (format "Missing env var: %s" k)]
         (throw (ex-info msg {:msg msg, :env-var k})))))
  ([k default]
   (or (System/getenv k) default)))

(def config
  {:region (get-env "AWS_REGION" :eu-west-1)
   :base-url (get-env "BASE_URL" "")
   :cloudfront-dist-id (get-env "CLOUDFRONT_DIST_ID")
   :entries-prefix (get-env "ENTRIES_PREFIX")
   :log-type (get-env "LOG_TYPE" :cloudfront)
   :num-days (util/->int (get-env "NUM_DAYS" "7"))
   :num-top-urls (util/->int (get-env "NUM_TOP_URLS" "10"))
   :s3-bucket (get-env "LOGS_BUCKET")
   :s3-prefix (get-env "LOGS_PREFIX")
   :views-table (get-env "VIEWS_TABLE")})

(log "Lambda starting" {:config config})

(def logs-client (logs/mk-client config))
(def views-client (page-views/client config))

(defn serve-dashboard [{:keys [queryStringParameters] :as event}]
  (let [date (:date queryStringParameters)
        start-date (:startDate queryStringParameters)
        end-date (:endDate queryStringParameters)
        dates (cond
                date
                [date]

                (and start-date end-date)
                (let [start-date (time/get-date start-date)
                      end-date (time/get-date end-date)]
                  (->> (iterate #(.plusDays % 1) start-date)
                       (take-while #(.isBefore % (.plusDays end-date 1)))
                       (map str)))

                :else
                (->> (range (:num-days config))
                     (map #(str (.minusDays (LocalDate/now) %)))))
        date-label (cond
                     date date
                     (and start-date end-date) (format "%s - %s" start-date end-date)
                     :else (format "last %d days" (:num-days config)))
        {:keys [base-url]} config
        all-views (mapcat #(page-views/get-views views-client %) dates)
        total-views (reduce + (map :views all-views))
        top-urls (->> all-views
                      (group-by :url)
                      (map (fn [[url views]]
                             [url (reduce + (map :views views))]))
                      (sort-by second)
                      reverse
                      (take (:num-top-urls config))
                      (map-indexed (fn [i [url views]]
                                     (assoc (->map url views) :rank (inc i)))))
        chart-id (str "div-" (UUID/randomUUID))
        chart-data (->> all-views
                        (group-by :date)
                        (map (fn [[date rows]]
                               {:date date
                                :views (reduce + (map :views rows))}))
                        (sort-by :date))
        chart-spec (json/generate-string
                    {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
                     :data {:values chart-data}
                     :mark {:type "bar"}
                     :width "container"
                     :height 300
                     :encoding {:x {:field "date"
                                    :type "nominal"
                                    :axis {:labelAngle -45}}
                                :y {:field "views"
                                    :type "quantitative"}}})
        tmpl-vars (->map date-label
                         total-views
                         base-url
                         top-urls
                         chart-id
                         chart-spec)]
    (util/log "Rendering dashboard" tmpl-vars)
    {:statusCode 200
     :headers {"Content-Type" "text/html"}
     :body (selmer/render (slurp "index.html")
                          tmpl-vars)}))

(defn serve-dashboard-path [{:keys [queryStringParameters] :as event}]
  (let [date (:date queryStringParameters)
        path (:path queryStringParameters)
        entries (get (logs/get-entries logs-client date) path)
        {:keys [base-url]} config
        tmpl-vars (->map date
                         base-url
                         path
                         entries)]
    (util/log "Rendering dashboard" (dissoc tmpl-vars :entries))
    {:statusCode 200
     :headers {"Content-Type" "text/html"}
     :body (selmer/render (slurp "by-path.html")
                          tmpl-vars)}))

(defn get-logs [{:keys [date limit] :as event}]
  (when-not date
    (error "Missing required parameter"
           {:site-analyser/error :missing-parameter
            :site-analyser/parameter "date"}))
  (let [{:keys [log-type]} config
        date (time/get-date date)
        limit (when limit (util/->int limit))
        client (merge logs-client (when limit {:limit limit}))
        entries (logs/get-log-entries client date log-type)]
    entries))

(defn backfill-visits [{:keys [overwrite] :as event}]
  (let [{:keys [entries]} (get-logs event)]
    (page-views/record-views! views-client entries overwrite)))

(defn handle-request
  ([]
   (handle-request {} {}))
  ([event]
   (handle-request event {}))
  ([{:keys [operation queryStringParameters requestContext] :as event} _context]
   (log "Invoked with event" {:event event})
   (try
     (if requestContext
       ;; HTTP request
       (let [{:keys [method path]} (:http requestContext)
             _ (util/log "Request" (->map method path))
             favicon-res (favicon/serve-favicon path)
             res (if favicon-res
                   favicon-res
                   (case [method path]
                     ["GET" "/dashboard"]
                     (serve-dashboard event)

                     ["GET" "/dashboard/by-path"]
                     (serve-dashboard-path event)

                     ["GET" "/logs"]
                     (let [logs (get-logs queryStringParameters)]
                       {"statusCode" 200
                        "body" (json/encode logs)})

                     ["POST" "/backfill"]
                     (let [res (backfill-visits queryStringParameters)]
                       {"statusCode" 200
                        "body" (json/encode res)})

                     {:statusCode 404
                      :headers {"Content-Type" "application/json"}
                      :body (json/generate-string {:msg (format "Resource not found: %s" path)})}))]
         (util/log "Sending response" (dissoc res :body))
         res)

       ;; Direct invocation
       (case operation
         "get-logs"
         (get-logs event)

         "backfill-visits"
         (backfill-visits event)

         (error "Invalid operation" {:handler/error :invalid-operation
                                     :operation operation})))
     (catch Exception e
       (when-not (ex-data e)
         (log (ex-message e) (-> e sci/stacktrace sci/format-stacktrace)))
       {"statusCode" 400
        "body" (ex-message e)}))))
