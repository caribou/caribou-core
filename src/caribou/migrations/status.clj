(ns caribou.migrations.status
  (:require [caribou.db :as db]
            [caribou.util :as util]
            [caribou.config :as config]
            [caribou.model :as model]
            [caribou.migrations.bootstrap :as bootstrap]))

(defn drop-status [m]
  (if-let [status-field (model/pick :field {:where {:name "Status" :type "integer" :model_id (:id m)}})]
    (do
      (println "Dropping status from" (:slug m))
      (model/update :model (:id m) {:removed_fields (str (:id status-field))}))))

(defn remove-status-field []
  (println "Removing status fields")
  (doseq [m (model/gather :model)]
    (drop-status m)))

(defn update-db-for-status []
  (config/init)
  (model/init)
  (model/db
    (fn []
      (remove-status-field)
      (model/create :model bootstrap/status)
      (bootstrap/create-default-status)
      (doseq [m (model/gather :model)] (model/add-status-to-model m)))))