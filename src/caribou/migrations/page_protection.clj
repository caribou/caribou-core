(ns caribou.migrations.page-protection
  (:require [caribou.model :as model]))

(defn migrate
  []
  (model/update :page {:fields [{:name "Protected" :type "boolean"}
                                {:name "Username" :type "string"}
                                {:name "Password" :type "string"}]}))

(defn rollback
  [])