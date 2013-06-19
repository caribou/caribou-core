(ns caribou.field.position
  (:require [caribou.field :as field]
            [caribou.db :as db]
            [caribou.validation :as validation]
            [caribou.util :as util]
            [caribou.field.integer :as int]))

(defn position-update-values
  [field content values]
  (println)
  (clojure.pprint/pprint ["update position" :field field :content content
                          :values values])
  (println)
  (let [slug (-> field :row :slug)
        key (keyword slug)
        update (int/integer-update-values field content values)
        val (or (get update key) (get values key))
        model-id (-> field :row :model-id)
        model (field/models model-id :slug)]
    (if val
      update
      (assoc update
        key (let [result
                  (db/query
                   (str "select max(" (util/dbize slug)
                        ")+1 as max from " (util/dbize model)))]
              (or (-> result first :max)
                  0))))))
                    
(defrecord PositionField [row env]
  field/Field
  (table-additions [this field] [[(keyword field) "INTEGER"]])
  (subfield-names [this field] [])
  (setup-field [this spec] nil)
  (rename-model [this old-slug new-slug])
  (rename-field [this old-slug new-slug])
  (cleanup-field [this]
    (field/field-cleanup this))
  (target-for [this] nil)
  (update-values [this content values]
    (position-update-values this content values))
  (post-update [this content opts] content)
  (pre-destroy [this content] content)
  (join-fields [this prefix opts] [])
  (join-conditions [this prefix opts] [])
  (build-where
    [this prefix opts]
    (field/integer-where this prefix opts))
  (natural-orderings [this prefix opts])
  (build-order [this prefix opts]
    (field/pure-order this prefix opts))
  (field-generator [this generators]
    (assoc generators (keyword (:slug row)) (fn [] #_(rand-int 777777777))))
  (fuse-field [this prefix archetype skein opts]
    (field/pure-fusion this prefix archetype skein opts))
  (localized? [this] true)
  (propagate-order [this id orderings])
  (models-involved [this opts all]
    (field/id-models-involved this opts all))
  (field-from [this content opts] (content (keyword (:slug row))))
  (render [this content opts] content)
  (validate [this opts] (validation/for-type this opts integer? "integer")))

(defn constructor
  [row]
  (PositionField. row {}))
