(ns caribou.field.tie
  (:require [caribou.field :as field]
            [caribou.util :as util]
            [caribou.db :as db]
            [caribou.validation :as validation]
            [caribou.association :as assoc]))

(defrecord TieField [row env]
  field/Field

  (table-additions [this field] [])
  (subfield-names [this field] [(str field "_id")])

  (setup-field [this spec]
    (let [model_id (:model_id row)
          model (@field/models model_id)
          id-slug (str (:slug row) "_id")]
      ((resolve 'caribou.model/update) :model model_id
        {:fields
         [{:name (util/titleize id-slug)
           :type "integer"
           :editable false
           :reference (:slug model)}]} {:op :migration})))

  (rename-model [this old-slug new-slug]
    (field/rename-model-index old-slug new-slug (str (-> this :row :slug) "_id")))

  (rename-field [this old-slug new-slug]
    (field/rename-index this (str old-slug "_id") (str new-slug "_id")))

  (cleanup-field [this]
    (let [model (@field/models (row :model_id))
          fields (:fields model)
          id-slug (keyword (str (:slug row) "_id"))]
      (db/drop-index (:slug model) id-slug)
      ((resolve 'caribou.model/destroy) :field (-> fields id-slug :row :id))))

  (target-for [this] this)

  (update-values [this content values] values)

  (post-update [this content opts] content)

  (pre-destroy [this content] content)

  (join-fields [this prefix opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (let [target (@field/models (:model_id row))]
          (assoc/model-select-fields target (str prefix "$" (:slug row))
                                     down)))))

  (join-conditions [this prefix opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (let [target (@field/models (:model_id row))
              id-slug (keyword (str (:slug row) "_id"))
              id-field (-> target :fields id-slug)
              table-alias (str prefix "$" (:slug row))
              field-select (field/coalesce-locale target id-field prefix
                                                   (name id-slug) opts)
              downstream (assoc/model-join-conditions target table-alias down)
              params [(:slug target) table-alias field-select]]
          (concat
           [(util/clause "left outer join %1 %2 on (%3 = %2.id)" params)]
           downstream)))))

  (build-where
    [this prefix opts]
    (assoc/with-propagation :where opts (:slug row)
      (fn [down]
        (let [target (@field/models (:model_id row))]
          (assoc/model-where-conditions target (str prefix "$" (:slug row)) down)))))

  (natural-orderings [this prefix opts]
    (assoc/with-propagation :where opts (:slug row)
      (fn [down]
        (let [target (@field/models (:model_id row))]
          (assoc/model-natural-orderings target (str prefix "$" (:slug row)) down)))))

  (build-order [this prefix opts]
    (assoc/join-order this (@field/models (:model_id row)) prefix opts))

  (field-generator [this generators]
    generators)

  (fuse-field [this prefix archetype skein opts]
    (assoc/part-fusion this (@field/models (-> this :row :model_id)) prefix archetype skein opts))

  (localized? [this] false)

  (models-involved [this opts all]
    (if-let [down (assoc/with-propagation :include opts (:slug row)
                    (fn [down]
                      (let [target (@field/models (:model_id row))]
                        (assoc/model-models-involved target down all))))]
      down
      all))

  (field-from [this content opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (if-let [tie-key (keyword (str (:slug row) "_id"))]
          (let [model (@field/models (:model_id row))]
            (assoc/from model (db/choose (:slug model) (content tie-key)) down))))))

  (render [this content opts]
    (assoc/part-render this (@field/models (:model_id row)) content opts))

  (validate [this opts] (validation/for-assoc this opts)))

(defn constructor
  [row]
  (TieField. row {}))
  

