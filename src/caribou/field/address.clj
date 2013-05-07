(ns caribou.field.address
  (:require [clojure.string :as string]
            [geocoder.core :as geo]
            [caribou.field :as field]
            [caribou.util :as util]
            [caribou.db :as db]
            [caribou.validation :as validation]
            [caribou.association :as assoc]))

(defn full-address [address]
  (string/join " " [(address :address)
             (address :address-two)
             (address :city)
             (address :state)
             (address :postal-code)
             (address :country)]))

(defn geocode-address [address]
 (let [code (geo/geocode-address (full-address address))]
   (if (empty? code)
     {}
     {:lat (-> (first code) :location :latitude)
      :lng (-> (first code) :location :longitude)})))

(defrecord AddressField [row env]
  field/Field
  (table-additions [this field] [])
  (subfield-names [this field] [(str field "-id")])
  (setup-field [this spec]
    (let [id-slug (str (:slug row) "-id")]
      ((resolve 'caribou.model/update) :model (:model-id row)
       {:fields [{:name (util/titleize id-slug)
                  :type "integer"
                  :editable false
                  :reference :location}]} {:op :migration})))

  (rename-model [this old-slug new-slug]
    (field/rename-model-index old-slug new-slug (str (-> this :row :slug) "-id")))

  (rename-field [this old-slug new-slug]
    (field/rename-index this (str old-slug "-id") (str new-slug "-id")))

  (cleanup-field [this]
    (let [model (field/models (:model-id row))
          id-slug (-> row :slug (str "-id") keyword)]
      (db/drop-index (:slug model) id-slug)
      ((resolve 'caribou.model/destroy) :field (-> model :fields id-slug :row :id))))

  (target-for [this] nil)
  (update-values [this content values]
    (let [posted (content (keyword (:slug row)))
          idkey (keyword (str (:slug row) "-id"))
          preexisting (content idkey)
          address (if preexisting (assoc posted :id preexisting) posted)]
      (if address
        (let [geocode (geocode-address address)
              location ((resolve 'caribou.model/create) :location
                        (merge address geocode))]
          (assoc values idkey (location :id)))
        values)))
  (post-update [this content opts] content)
  (pre-destroy [this content] content)

  (join-fields [this prefix opts]
    (assoc/model-select-fields (field/models :location)
                               (str prefix "$" (:slug row))
                               (dissoc opts :include)))

  (join-conditions [this prefix opts]
    (let [model (field/models (:model-id row))
          slug (:slug row)
          id-slug (keyword (str slug "-id"))
          id-field (-> model :fields id-slug)
          field-select (field/coalesce-locale model id-field prefix
                                               (name id-slug) opts)]
      [(util/clause "left outer join location %2$%1 on (%3 = %2$%1.id)"
                    [(util/dbize (:slug row)) (util/dbize prefix) field-select])]))

  (build-where
    [this prefix opts]
    (assoc/with-propagation :where opts (:slug row)
      (fn [down]
        (assoc/model-where-conditions (field/models :location)
                                      (str prefix "$" (:slug row)) down))))

  (natural-orderings [this prefix opts])

  (build-order [this prefix opts]
    (assoc/join-order this (field/models :location) prefix opts))

  (field-generator [this generators]
    generators)

  (fuse-field [this prefix archetype skein opts]
    (assoc/join-fusion this (field/models :location) prefix archetype skein opts))

  (localized? [this] false)
  (models-involved [this opts all] all)

  (propagate-order [this id orderings])
  (field-from [this content opts]
    (or (db/choose :location (content (keyword (str (:slug row) "-id")))) {}))

  (render [this content opts]
    (assoc/join-render this (field/models :location) content opts))
  (validate [this opts] (validation/for-address this opts)))

(defn constructor
  [row]
  (AddressField. row {}))
