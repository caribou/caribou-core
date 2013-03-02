(ns caribou.field.integer
  (:require [caribou.util :as util]
            [caribou.field :as field]
            [caribou.validation :as validation]))

(defn integer-update-values
  [field content values]
  (let [key (-> field :row :slug keyword)]
    (if (contains? content key)
      (let [value (get content key)
            tval (util/convert-int value)]
        (assoc values key tval))
      values)))

(defrecord IntegerField [row env]
  field/Field
  (table-additions [this field] [[(keyword field) :integer]])
  (subfield-names [this field] [])
  (setup-field [this spec] nil)
  (rename-model [this old-slug new-slug])
  (rename-field [this old-slug new-slug])
  (cleanup-field [this]
    (field/field-cleanup this))
  (target-for [this] nil)

  (update-values [this content values]
    (integer-update-values this content values))

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
    (assoc generators (keyword (:slug row)) (fn [] (rand-int 777777777))))
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
  (IntegerField. row {}))
