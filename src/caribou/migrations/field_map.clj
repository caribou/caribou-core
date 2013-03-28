(ns caribou.migrations.field-map
  (:require [caribou.config :as config]
            [caribou.model :as model]))

(defn migrate
  []
  (model/update
   :model
   (-> @model/models :field :id)
   {:fields [{:name "Map" :type "boolean"}]}))

(defn rollback
  [])