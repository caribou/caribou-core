(ns caribou.migrations.page-siphon
  (:require [caribou.config :as config]
            [caribou.model :as model]
            [caribou.migrations.bootstrap :as bootstrap]))

(defn migrate
  []
  (let [siphon (model/create
                :model
                {:name "Siphon"
                 :description "A specification of content that can be loaded in dynamically."
                 :locked true
                 :fields [{:name "Spec" :type "structure"}]})]

    (model/update
     :model
     (model/models :page :id)
     {:fields [{:name "Siphons"
                :type "collection"
                :target-id (:id siphon)
                :map true}]})))

(defn rollback
  [])