(ns caribou.migrations.status
  (:require [caribou.db :as db]
            [caribou.util :as util]
            [caribou.model :as model]))

(defn create-status-model []
  (model/create :model {:name "Status"
             						:description "all possible states a model instance can have"
             						:locked true
             						:fields [{:name "Name" :type "string" :locked true}
                      					     {:name "Description" :type "text" :locked true}
                      					     {:name "Value" :type "integer" :locked true}]}))

(defn add-default-status []
	"Add default statuses that will be available for all models, always"
	(db/insert :status {:value 1
						:name "Draft"
	               		:description "Draft status is not publicly visible"
	               		:locked true
	               		:updated_at (model/current-timestamp)
	               		})
	(db/insert :status {:value 2
						:name "Published"
	               		:description "Published means visible to the public"
	               		:locked true
	               		:updated_at (model/current-timestamp)
	               		}))

(defn alter-status-definition
	"Change existing models to use an association instead of plain integer for the status?"
	[])

(defn update-db-for-status []
  (create-status-model)
  (add-default-status)
  (alter-status-definition))