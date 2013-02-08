(ns caribou.migrations.page-protection
  (:require [caribou.config :as config]
            [caribou.model :as model]))

(defn migrate
  []
  (config/init)
  (model/init)
  (model/update
   :model
   (-> @model/models :page :id)
   {:fields [{:name "Protected" :type "boolean"}
             {:name "Username" :type "string"}
             {:name "Password" :type "password"}]}))

(defn rollback
  [])