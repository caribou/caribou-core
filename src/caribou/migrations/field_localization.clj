(ns caribou.migrations.field-localization
  (:require [caribou.config :as config]
            [caribou.model :as model]
            [caribou.migrations.bootstrap :as bootstrap]))

(defn migrate
  []
  (model/update
   :model
   (model/models :field :id)
   {:fields [{:name "localized"
              :type "boolean"}]}))

(defn rollback
  [])