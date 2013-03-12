(ns caribou.field.position
  (:require [caribou.field :as field]
            [caribou.validation :as validation]
            [caribou.util :as util]
            [caribou.field.integer :as int]))

(defn update
  [field content values row]
  (let [slug (-> field :row :slug)
        key (keyword slug)
        update (int/integer-update-values field content values)
        val (get update key)
        model-id (:model_id row)
        model (-> model-id (@field/models) :slug)]
    (if val
      update
      (assoc update key
             (let [result (util/query
                           (str "select max(" slug ")+1 as max from "
                                model))]
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
    (update this content values row))
  (post-update [this content opts] content)
  (pre-destroy [this content] content)
  (join-fields [this prefix opts] [])
  (join-conditions [this prefix opts] [])
  (build-where
    [this prefix opts]
    (field/field-where this prefix opts field/pure-where))
  (natural-orderings [this prefix opts])
  (build-order [this prefix opts]
    (field/pure-order this prefix opts))
  (field-generator [this generators]
    (assoc generators (keyword (:slug row)) (fn [] #_(rand-int 777777777))))
  (fuse-field [this prefix archetype skein opts]
    (field/pure-fusion this prefix archetype skein opts))
  (localized? [this] true)
  (models-involved [this opts all]
    (field/id-models-involved this opts all))
  (field-from [this content opts] (content (keyword (:slug row))))
  (render [this content opts] content)
  (validate [this opts] (validation/for-type this opts integer? "integer")))

(defn constructor
  [row]
  (PositionField. row {}))
