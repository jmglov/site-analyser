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

(defn entry->dynamo-item [{:keys [date ip path referer request-id time user-agent]}]
  {:Item (merge {"date" {:S date}
                 "url" {:S (format "url:%s" path)}
                 "time" {:S time}
                 "request-id" {:S (format "%sT%sZ;%s" date time request-id)}
                 "client-ip" {:S ip}
                 "user-agent" {:S user-agent}}
                (when referer
                  (->map referer)))})

(defn entry->dynamo-update [{:keys [date path]}]
  {:Key {:date {:S date}
         :url {:S path}}
   :UpdateExpression "ADD #views :increment SET #reqid = :reqid"
   :ExpressionAttributeNames {"#views" "views", "#reqid" "request-id"}
   :ExpressionAttributeValues {":increment" {:N "1"}
                               ":reqid" {:S "count"}}})

(defn record-view! [{:keys [dynamodb views-table] :as client} entry]
  (let [dynamo-item (assoc (entry->dynamo-item entry)
                           :TableName views-table)
        dynamo-update (assoc (entry->dynamo-update entry)
                             :TableName views-table)]
    (aws/invoke dynamodb {:op :TransactWriteItems
                          :request {:TransactItems
                                    [{:Put dynamo-item}
                                     {:Update dynamo-update}]}})))
