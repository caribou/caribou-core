(ns caribou.migrations.uuid
  (:require [caribou.util :as util]
            [caribou.db :as db]
            [caribou.model :as model]))

(defn migrate
  []
  (let [models (model/gather :model)]
    (doseq [model (remove #(contains? (:fields (model/models (:id %))) :uuid) models)]
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
          (db/update model-slug ["id=?" item-id] {:uuid (util/random-uuid)}))))
    
    (doseq [tie (model/gather :field {:where {:type "tie"}})]
      (model/update :field (:id tie) {:target-id (:model-id tie)}))))

(defn rollback
  [])
