(ns caribou.migrations.model-field-dbslugs
  (:require [caribou.util :as util]
            [caribou.db :as db]))

(defn migrate
  []
  (let [model (first (db/fetch :model "slug = ?" "model"))
        field (first (db/fetch :model "slug = ?" "field"))
        system-slugs (db/fetch :field "type = ? and (model_id = ? or model_id = ?)" "slug" (:id model) (:id field))]
    (doseq [system-slug system-slugs]
      (db/update :field ["id = ?" (:id system-slug)] {:type "dbslug"}))))

(defn rollback
  [])
