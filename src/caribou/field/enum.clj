(ns caribou.field.enum
  (:require [caribou.util :as util]
            [caribou.validation :as validation]
            [caribou.field :as field]
            [caribou.db :as db]
            [caribou.association :as assoc]))

(defn enum-fusion
  [this prefix archetype skein opts]
  (assoc/join-fusion
   this (field/models :enumeration) prefix archetype skein opts
   (fn [master]
     (if master
       (get master :entry)))))

(defn enum-update-values
  [field content values original]
  (let [slug (-> field :row :slug)
        key (keyword slug)]
    (if (contains? content key)
      (let [value (get content key)
            found (some
                   (fn [enum]
                     (if (= value (:entry enum))
                       (:id enum)))
                   (-> field :row :enumerations))]
        (assoc values
          (keyword (str slug "-id")) found))
      values)))

(defrecord EnumField [row env]
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
                  :reference :enumeration}]} {:op :migration})))

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
    (enum-update-values this content values original))
  (post-update [this content opts] content)
  (pre-destroy [this content] content)

  (join-fields [this prefix opts]
    (assoc/model-select-fields
     (field/models :enumeration)
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
      [{:table ["enumeration" table-alias]
        :on [field-select (str table-alias ".id")]}]))

  (build-where
    [this prefix opts]
    (assoc/with-propagation :where opts (:slug row)
      (fn [down]
        (assoc/model-where-conditions
         (field/models :enumeration)
         (str prefix "$" (:slug row))
         down))))

  (natural-orderings [this prefix opts])

  (build-order [this prefix opts]
    (assoc/join-order this (field/models :enumeration) prefix opts))

  (field-generator [this generators]
    generators)

  (fuse-field [this prefix archetype skein opts]
    (enum-fusion this prefix archetype skein opts))

  (localized? [this] false)

  (models-involved [this opts all] all)

  (field-from [this content opts]
    (let [enumeration-id (content (keyword (str (:slug row) "-id")))
          enumeration (or (db/choose :enumeration enumeration-id) {})]
      (get enumeration :entry)))

  (propagate-order [this id orderings])
  (render [this content opts]
    (assoc/join-render this (field/models :enumeration) content opts))
  (validate [this opts] (validation/for-asset this opts)))

(defn constructor
  [row]
  (let [enumerations (db/query "select id, entry from enumeration where field_id = ?" [(get row :id)])]
    (EnumField. (assoc row :enumerations enumerations) {})))
