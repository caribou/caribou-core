(ns caribou.migrations.enumeration
  (:require [caribou.config :as config]
            [caribou.model :as model]))

(defn migrate
  []

  (model/create
   :model
   {:name "Enumeration"
    :fields [{:name "entry"
              :type "string"}]})

  (let [enum (model/models :enumeration)]
    (model/update
     :model
     (model/models :field :id)
     {:fields [{:name "Enumerations"
                :type "collection"
                :reciprocal-name "Field"
                :target-id (:id enum)}]})))

(defn rollback
  [])