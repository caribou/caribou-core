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
   this (field/models :asset) prefix archetype skein opts
   (fn [master]
     (if master
       (assoc master :path (asset/asset-path master))))))

(defn commit-asset-source
  [field content values original]
  (let [row (:row field)
        slug (:slug row)
        posted (get content (keyword slug))
        id-key (keyword (str slug "-id"))]
        ;; preexisting (get original id-key)
        ;; posted (if preexisting 
        ;;          (assoc posted :id preexisting)
        ;;          posted)]
    (if posted
      (let [asset ((resolve 'caribou.model/create) :asset posted)]
        (assoc values id-key (:id asset)))
      values)))

(defrecord AssetField [row env]
  field/Field
  (table-additions [this field] [])
  (subfield-names [this field] [(str field "-id")])
  (setup-field [this spec]
    (let [id-slug (str (:slug row) "-id")
          localized (-> this :row :localized)]
      ((resolve 'caribou.model/update) :model (:model-id row)
       {:fields [{:name (util/titleize id-slug)
                  :type "integer"
                  :localized localized
                  :editable false
                  :reference :asset}]} {:op :migration})))

  (rename-model [this old-slug new-slug]
    (field/rename-model-index old-slug new-slug (str (-> this :row :slug) "-id")))

  (rename-field [this old-slug new-slug]
    (field/rename-index this (str old-slug "-id") (str new-slug "-id")))

  (cleanup-field [this]
    (let [model (field/models (:model-id row))
          id-slug (keyword (str (:slug row) "-id"))]
      (db/drop-index (:slug model) id-slug)
      ((resolve 'caribou.model/destroy) :field (-> model :fields id-slug :row :id))))

  (target-for [this] nil)
  (update-values [this content values original] 
    (commit-asset-source this content values original))

  (post-update [this content opts] content)
  (pre-destroy [this content] content)

  (join-fields [this prefix opts]
    (assoc/model-select-fields (field/models :asset)
                                     (str prefix "$" (:slug row))
                                     (dissoc opts :include)))

  (join-conditions [this prefix opts]
    (let [model (field/models (:model-id row))
          slug (:slug row)
          id-slug (keyword (str slug "-id"))
          id-field (-> model :fields id-slug)
          field-select (field/coalesce-locale
                        model id-field prefix
                        (name id-slug) opts)
          table-alias (str prefix "$" (:slug row))]
      [{:table ["asset" table-alias]
        :on [field-select (str table-alias ".id")]}]))

  (build-where
    [this prefix opts]
    (assoc/with-propagation :where opts (:slug row)
      (fn [down]
        (assoc/model-where-conditions (field/models :asset)
                                            (str prefix "$" (:slug row))
                                            down))))

  (natural-orderings [this prefix opts])

  (build-order [this prefix opts]
    (assoc/join-order this (field/models :asset) prefix opts))

  (field-generator [this generators]
    generators)

  (fuse-field [this prefix archetype skein opts]
    (asset-fusion this prefix archetype skein opts))

  (localized? [this] false)

  (models-involved [this opts all] all)

  (field-from [this content opts]
    (let [asset-id (content (keyword (str (:slug row) "-id")))
          asset (or (db/choose :asset asset-id) {})]
      (assoc asset :path (asset/asset-path asset))))

  (propagate-order [this id orderings])
  (render [this content opts]
    (assoc/join-render this (field/models :asset) content opts))
  (validate [this opts] (validation/for-asset this opts)))

(defn constructor
  [row]
  (AssetField. row {}))
