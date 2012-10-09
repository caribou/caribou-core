(ns ^{:skip-wiki true}
  caribou.migrations.premigrations
  (:require [caribou.db :as db]
            [caribou.util :as util]
            [caribou.model :as model]))

(defn update-db-for-localization
  []
  (let [model-id ((first (util/query "select id from model where slug = 'model'")) :id)]
    (db/add-column :model :localized [:boolean "DEFAULT false"])
    (db/insert :field {:name "Localized"
                       :slug "localized"
                       :type "boolean"
                       :locked true
                       :updated_at (model/current-timestamp)
                       :model_id model-id})))