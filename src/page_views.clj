(ns page-views
  (:require [com.grzm.awyeah.client.api :as aws]
            [util :refer [->map]]))

(defn client [{:keys [aws-region] :as config}]
  (assoc config :dynamodb (aws/client {:api :dynamodb, :region aws-region})))

(defn validate-response [res]
  (when (:cognitect.anomalies/category res)
    (let [data (merge (select-keys res [:cognitect.anomalies/category])
                      {:err-msg (:Message res)
                       :err-type (:__type res)})]
      (util/log "DynamoDB request failed" data)
      (throw (ex-info "DynamoDB request failed" data))))
  res)

(defn increment! [{:keys [dynamodb views-table] :as client} date url]
  (let [req {:TableName views-table
             :Key {:date {:S date}
                   :url {:S url}}
             :UpdateExpression "ADD #views :increment SET #reqid = :reqid"
             :ExpressionAttributeNames {"#views" "views", "#reqid" "request-id"}
             :ExpressionAttributeValues {":increment" {:N "1"}
                                         ":reqid" {:S "count"}}
             :ReturnValues "ALL_NEW"}
        _ (util/log "Incrementing page view counter"
                    (->map date url req))
        res (-> (aws/invoke dynamodb {:op :UpdateItem
                                      :request req})
                validate-response)
        new-counter (-> res
                        (get-in [:Attributes :views :N])
                        util/->int)
        ret (->map date url new-counter)]
    (util/log "Page view counter incremented"
              ret)
    ret))

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
                  validate-response)
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

(defn request->dynamo-item [{:keys [date time ip path user-agent request-id]}]
  {:Item {"date" {:S date}
          "url" {:S (format "url:%s" path)}
          "request-id" {:S (format "%sT%sZ;%s" date time request-id)}
          "client-ip" {:S ip}
          "user-agent" {:S user-agent}}})

(defn request->dynamo-update [{:keys [date path]}]
  {:Key {:date {:S date}
         :url {:S path}}
   :UpdateExpression "ADD #views :increment SET #reqid = :reqid"
   :ExpressionAttributeNames {"#views" "views", "#reqid" "request-id"}
   :ExpressionAttributeValues {":increment" {:N "1"}
                               ":reqid" {:S "count"}}})

(defn record-visit! [{:keys [dynamodb views-table] :as client} request]
  (let [dynamo-item (assoc (request->dynamo-item request)
                           :TableName views-table)
        dynamo-update (assoc (request->dynamo-update request)
                             :TableName views-table)]
    (aws/invoke dynamodb {:op :TransactWriteItems
                          :request {:TransactItems
                                    [{:Put dynamo-item}
                                     {:Update dynamo-update}]}})))
