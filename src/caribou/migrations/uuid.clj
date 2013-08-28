(ns caribou.migrations.uuid
  (:require [caribou.util :as util]
            [caribou.db :as db]
            [caribou.model :as model]))

(defn migrate
  []
  (let [models (model/gather :model)]
    (doseq [model models]
      (model/update
       :model
       (:id model)
       {:fields [{:name "UUID" 
                  :slug "uuid" 
                  :type "string" 
                  :locked true 
                  :immutable true 
                  :editable false}]})
      (db/create-index (:slug model) "uuid"))
    (doseq [model-slug (map (comp keyword :slug) models)]
      (let [content (model/gather model-slug)]
        (doseq [item-id (map :id content)]
          (model/update
           model-slug
           item-id
           {:uuid (util/random-uuid)}))))))

(defn rollback
  [])
