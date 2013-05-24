(ns caribou.migrations.add-text-format
  (:require [caribou.db :as db]
            [caribou.config :as config]
            [caribou.logger :as log]
            [caribou.util :as util]
            [caribou.model :as model]))

(defn migrate [] 
  (db/add-column :field :format ["varchar(32)" "DEFAULT NULL"])
  (let [model-id (-> (db/query "select id from model where slug = ?" ["field"]) first :id)]
    (db/insert
       :field
       {:name "Format"
        :slug "format"
        :type "string"
        :locked true
        :editable true
        :updated-at (model/current-timestamp)
        :model-id model-id})))

(defn rollback []
  (let [model-id (-> (db/query "select id from model where slug = ?" ["field"]) first :id)]
    (db/delete :field "model_id = ? AND slug = ?" model-id "format"))
  (db/drop-column :field :format))
