(ns caribou.migrations.model-display
  (:require [clojure.java.jdbc :as sql]
            [caribou.model :as model]))

(defn migrate
  []
  (model/update 
   :model 
   (:id (model/models :model)) 
   {:fields 
    [{:name "Display" 
      :type "structure"
      :locked true}]}))
  
(defn rollback
  [])
