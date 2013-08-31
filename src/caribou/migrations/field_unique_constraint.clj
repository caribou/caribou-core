(ns caribou.migrations.field-unique-constraint
  (:require [clojure.java.jdbc :as sql]
            [caribou.model :as model]))

(defn migrate
  []
  (let [status-model-id (:id (model/models :status))
        status-self-collection (model/pick 
                                :field 
                                {:where {:model-id status-model-id
                                         :slug "status"
                                         :type "collection"}})]
    (model/update :field (:id status-self-collection) {:name "Status Status"}))
  (sql/do-commands "alter table field add constraint model_id_slug_unique unique (model_id, slug)"))

(defn rollback
  [])
