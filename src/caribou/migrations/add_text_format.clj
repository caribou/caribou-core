(ns caribou.migrations.add-text-format
  (:require [caribou.db :as db]
            [caribou.config :as config]
            [caribou.logger :as log]
            [caribou.util :as util]
            [caribou.model :as model]))

(defn migrate [] 
  (db/add-column :field :format ["varchar(32)" "DEFAULT NULL"])
  (let [model-id ((first (util/query "select id from model where slug = 'field'")) :id)]
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
  (let [model-id ((first (util/query "select id from model where slug = 'field'")) :id)]
    (db/delete :field "model_id = %1 AND slug = '%2'" model-id "format"))
  (db/drop-column :field :format))
