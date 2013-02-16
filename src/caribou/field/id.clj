(ns caribou.field.id
  (:require [caribou.db :as db]
            [caribou.validation :as validation]
            [caribou.field-protocol :as field]))

(defrecord IdField [row env]
  field/Field
  (table-additions [this field] [[(keyword field) "SERIAL" "PRIMARY KEY"]])
  (subfield-names [this field] [])
  (setup-field [this spec models]
    (let [model (db/find-model (:model_id row) models)]
      (db/create-index (:slug model) (:slug row))))
  (rename-field [this old-slug new-slug])
  (cleanup-field [this] nil)
  (target-for [this] nil)
  (update-values [this content values] values)
  (post-update [this content opts] content)
  (pre-destroy [this content] content)
  (join-fields [this prefix opts] [])
  (join-conditions [this prefix opts] [])
  (build-where
    [this prefix opts models]
    (field/field-where this prefix opts field/pure-where models))
  (natural-orderings [this prefix opts])
  (build-order [this prefix opts models]
    (field/pure-order this prefix opts models))
  (field-generator [this generators]
    generators)
  (fuse-field [this prefix archetype skein opts]
    (field/pure-fusion this prefix archetype skein opts))
  (localized? [this] false)
  (models-involved [this opts all]
    (field/id-models-involved this opts all))
  (field-from [this content opts] (content (keyword (:slug row))))
  (render [this content opts] content)
  (validate [this opts models] (validation/for-type this opts integer? "id")))

(field/add-constructor :id (fn [row] (IdField. row {})))
