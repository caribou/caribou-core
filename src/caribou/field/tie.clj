(ns caribou.field.tie
  (:require [caribou.field :as field]
            [caribou.util :as util]
            [caribou.db :as db]
            [caribou.validation :as validation]
            [caribou.association :as assoc]))

(defrecord TieField [row env]
  field/Field

  (table-additions [this field] [])
  (subfield-names [this field] [(str field "-id")])

  (setup-field [this spec]
    (let [model-id (:model-id row)
          model (field/models model-id)
          id-slug (str (:slug row) "-id")
          tie-id (util/convert-int (:id row))]
      ((resolve 'caribou.model/update) :model model-id
        {:fields
         [{:name (util/titleize id-slug)
           :type "integer"
           :editable false
           :reference (:slug model)}]} {:op :migration})
      (db/update :field ["id = ?" tie-id] {:target-id model-id})))

  (rename-model [this old-slug new-slug]
    (field/rename-model-index old-slug new-slug (str (-> this :row :slug) "-id")))

  (rename-field [this old-slug new-slug]
    (field/rename-index this (str old-slug "-id") (str new-slug "-id")))

  (cleanup-field [this]
    (let [model (field/models (row :model-id))
          fields (:fields model)
          id-slug (keyword (str (:slug row) "-id"))]
      (db/drop-index (:slug model) id-slug)
      ((resolve 'caribou.model/destroy) :field (-> fields id-slug :row :id))))

  (target-for [this] this)

  (update-values [this content values original] values)

  (post-update [this content opts] content)

  (pre-destroy [this content] content)

  (join-fields [this prefix opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (let [target (field/models (:model-id row))]
          (assoc/model-select-fields target (str prefix "$" (:slug row))
                                     down)))))

  (join-conditions [this prefix opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (let [target (field/models (:model-id row))
              id-slug (keyword (str (:slug row) "-id"))
              id-field (-> target :fields id-slug)
              table-alias (str prefix "$" (:slug row))
              field-select (field/coalesce-locale
                            target id-field prefix
                            (name id-slug) opts)
              downstream (assoc/model-join-conditions target table-alias down)]
          (cons
           {:table [(:slug target) table-alias]
            :on [field-select (str table-alias ".id")]}
           downstream)))))

  (build-where
    [this prefix opts]
    (assoc/with-propagation :where opts (:slug row)
      (fn [down]
        (let [target (field/models (:model-id row))]
          (assoc/model-where-conditions target (str prefix "$" (:slug row)) down)))))

  (natural-orderings [this prefix opts]
    (assoc/with-propagation :where opts (:slug row)
      (fn [down]
        (let [target (field/models (:model-id row))]
          (assoc/model-natural-orderings target (str prefix "$" (:slug row)) down)))))

  (build-order [this prefix opts]
    (assoc/join-order this (field/models (:model-id row)) prefix opts))

  (field-generator [this generators]
    generators)

  (fuse-field [this prefix archetype skein opts]
    (assoc/part-fusion this (field/models (-> this :row :model-id)) prefix archetype skein opts))

  (localized? [this] false)

  (propagate-order [this id orderings])
  (models-involved [this opts all]
    (if-let [down (assoc/with-propagation :include opts (:slug row)
                    (fn [down]
                      (let [target (field/models (:model-id row))]
                        (assoc/model-models-involved target down all))))]
      down
      all))

  (field-from [this content opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (if-let [tie-key (keyword (str (:slug row) "-id"))]
          (let [model (field/models (:model-id row))]
            (assoc/from model (db/choose (:slug model) (content tie-key)) down))))))

  (render [this content opts]
    (assoc/part-render this (field/models (:model-id row)) content opts))

  (validate [this opts] (validation/for-assoc this opts)))

(defn constructor
  [row]
  (TieField. row {}))
  

