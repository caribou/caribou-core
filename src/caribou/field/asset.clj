(ns caribou.field.asset
  (:require [caribou.util :as util]
            [caribou.validation :as validation]
            [caribou.asset :as asset]
            [caribou.field :as field]
            [caribou.db :as db]
            [caribou.association :as assoc]))

(defn asset-fusion
  [this prefix archetype skein opts]
  (assoc/join-fusion
   this (:asset @field/models) prefix archetype skein opts
   (fn [master]
     (if master
       (assoc master :path (asset/asset-path master))))))

(defrecord AssetField [row env]
  field/Field
  (table-additions [this field] [])
  (subfield-names [this field] [(str field "_id")])
  (setup-field [this spec]
    (let [id-slug (str (:slug row) "_id")
          model (db/find-model (:model_id row) @field/models)]
      ((resolve 'caribou.model/update) :model (:model_id row)
       {:fields [{:name (util/titleize id-slug)
                  :type "integer"
                  :editable false
                  :reference :asset}]} {:op :migration})))

  (rename-model [this old-slug new-slug]
    (field/rename-model-index old-slug new-slug (str (-> this :row :slug) "_id")))

  (rename-field [this old-slug new-slug]
    (field/rename-index this (str old-slug "_id") (str new-slug "_id")))

  (cleanup-field [this]
    (let [model (get @field/models (:model_id row))
          id-slug (keyword (str (:slug row) "_id"))]
      (db/drop-index (:slug model) id-slug)
      ((resolve 'caribou.model/destroy) :field (-> model :fields id-slug :row :id))))

  (target-for [this] nil)
  (update-values [this content values] values)
  (post-update [this content opts] content)
  (pre-destroy [this content] content)

  (join-fields [this prefix opts]
    (assoc/model-select-fields (:asset @field/models)
                                     (str prefix "$" (:slug row))
                                     (dissoc opts :include)))

  (join-conditions [this prefix opts]
    (let [model (@field/models (:model_id row))
          slug (:slug row)
          id-slug (keyword (str slug "_id"))
          id-field (-> model :fields id-slug)
          field-select (field/coalesce-locale model id-field prefix
                                               (name id-slug) opts)]
      [(util/clause "left outer join asset %2$%1 on (%3 = %2$%1.id)"
                    [(:slug row) prefix field-select])]))

  (build-where
    [this prefix opts]
    (assoc/with-propagation :where opts (:slug row)
      (fn [down]
        (assoc/model-where-conditions (:asset @field/models)
                                            (str prefix "$" (:slug row))
                                            down))))

  (natural-orderings [this prefix opts])

  (build-order [this prefix opts]
    (assoc/join-order this (:asset @field/models) prefix opts))

  (field-generator [this generators]
    generators)

  (fuse-field [this prefix archetype skein opts]
    (asset-fusion this prefix archetype skein opts))

  (localized? [this] false)

  (models-involved [this opts all] all)

  (field-from [this content opts]
    (let [asset-id (content (keyword (str (:slug row) "_id")))
          asset (or (db/choose :asset asset-id) {})]
      (assoc asset :path (asset/asset-path asset))))

  (propagate-order [this id orderings])
  (render [this content opts]
    (assoc/join-render this (:asset @field/models) content opts))
  (validate [this opts] (validation/for-asset this opts)))

(defn constructor
  [row]
  (AssetField. row {}))
