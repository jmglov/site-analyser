(ns page-views
  (:require [com.grzm.awyeah.client.api :as aws]
            [util :refer [->map]]))

(defn client [{:keys [region] :as config}]
  (assoc config :dynamodb (aws/client {:api :dynamodb, :region region})))

(defn get-query-page [{:keys [dynamodb views-table] :as client}
                      date
                      {:keys [page-num LastEvaluatedKey] :as prev}]
  (when prev
    (util/log "Got page" prev))
  (when (or (nil? prev)
            LastEvaluatedKey)
    (let [page-num (inc (or page-num 0))
          req (merge
               {:TableName views-table
                :KeyConditionExpression "#date = :date"
                :ExpressionAttributeNames {"#date" "date"}
                :ExpressionAttributeValues {":date" {:S date}}}
               (when LastEvaluatedKey
                 {:ExclusiveStartKey LastEvaluatedKey}))
          _ (util/log "Querying page views" (->map date page-num req))
          res (-> (aws/invoke dynamodb {:op :Query
                                        :request req})
                  util/validate-aws-response)
          _ (util/log "Got response" (->map res))]
      (assoc res :page-num page-num))))

(defn get-views [client date]
  (->> (iteration (partial get-query-page client date)
                  :vf :Items)
       util/lazy-concat
       (map (fn [{:keys [views date url]}]
              {:date (:S date)
               :url (:S url)
               :views (util/->int (:N views))}))))

(defn entry->dynamo-item
  [{:keys [log-file date ip path referer request-id time user-agent]}
   overwrite?]
  (let [compound-request-id (format "%sT%sZ;%s" date time request-id)]
    (merge
     {:Item (merge {"url" {:S (format "url:%s" path)}  ; GSI partition key
                    "request-id" {:S compound-request-id}  ; GSI sort key

                    ;; Since date:url is the table's composite key, it must be unique.
                    ;; Instead of using date, we'll use compound-request-id, which is
                    ;; formed by concatenating the datetime and the original request ID,
                    ;; which is guaranteed to be unique.
                    "date" {:S compound-request-id}  ; table partition key

                    "time" {:S time}
                    "client-ip" {:S ip}
                    "user-agent" {:S user-agent}
                    "log-file" {:S log-file}}
                   (when referer
                     (->map referer)))}
     (when-not overwrite?
       {:ConditionExpression "attribute_not_exists(#url)"
        :ExpressionAttributeNames {"#url" "url"}}))))

(defn entry->dynamo-update [{:keys [date path count]}]
  {:Key {:date {:S date}
         :url {:S path}}
   :UpdateExpression "ADD #views :increment SET #reqid = :reqid"
   :ExpressionAttributeNames {"#views" "views", "#reqid" "request-id"}
   :ExpressionAttributeValues {":increment" {:N (str count)}
                               ":reqid" {:S "count"}}})

(defn entries->dynamo-puts [table-name entries overwrite?]
  (map (fn [entry]
         {:Put (assoc (entry->dynamo-item entry overwrite?)
                      :TableName table-name)})
       entries))

(defn entries->dynamo-updates [table-name entries]
  (->> entries
       (group-by (juxt :date :path))
       (map (fn [[_ entries]]
              (assoc (first entries) :count (count entries))))
       (map (fn [entry]
              {:Update (assoc (entry->dynamo-update entry)
                              :TableName table-name)}))))

(defn conditional-check-failed? [res]
  (and (= :cognitect.anomalies/incorrect (:cognitect.anomalies/category res))
       (some #(= "ConditionalCheckFailed" (:Code %)) (:CancellationReasons res))))

(defn record-views!
  ([client entries]
   (record-views! client entries false))
  ([{:keys [dynamodb views-table] :as client} entries overwrite?]
   (->> entries
        (group-by :log-file)
        (mapcat
         (fn [[log-file entries]]
           ;; The max number of items in a Dynamo TransactItems request
           ;; is 100, and each entry generates two items (a Put and an
           ;; Update), so we need to chunk them 50 at a time.
           (->> (partition-all 50 entries)
                (map-indexed
                 (fn [i entries]
                   (let [log-file (format "%s.%s" log-file i)
                         entries (map #(assoc % :log-file log-file) entries)]
                     [log-file
                      (concat (entries->dynamo-puts views-table entries overwrite?)
                              (entries->dynamo-updates views-table entries))]))))))
        (map (fn [[log-file transact-items]]
               (let [req {:TransactItems transact-items}
                     _ (util/log "Recording views"
                                 {:log-file log-file
                                  :num-entries (/ (count transact-items) 2)})
                     res (aws/invoke dynamodb {:op :TransactWriteItems
                                               :request req})]
                 [log-file
                  (cond
                    (conditional-check-failed? res)
                    {::success? false, ::reason :already-recorded}

                    (:cognitect.anomalies/category res)
                    (do
                      (util/log "Dynamo request failed"
                                {:log-file log-file
                                 :request req
                                 :response res})
                      {::success? false, ::reason res})

                    :else
                    {::success true})])))
        (into {}))))
