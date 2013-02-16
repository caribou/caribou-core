(ns caribou.field.address
  (:require [clojure.string :as string]
            [geocoder.core :as geo]
            [caribou.field-protocol :as field]
            [caribou.util :as util]
            [caribou.db :as db]
            [caribou.validation :as validation]
            [caribou.model-association :as assoc]))

(defn full-address [address]
  (string/join " " [(address :address)
             (address :address_two)
             (address :city)
             (address :state)
             (address :postal_code)
             (address :country)]))

(defn geocode-address [address]
 (let [code (geo/geocode-address (full-address address))]
   (if (empty? code)
     {}
     {:lat (-> (first code) :location :latitude)
      :lng (-> (first code) :location :longitude)})))

(defrecord AddressField [row env operations]
  field/Field
  (table-additions [this field] [])
  (subfield-names [this field] [(str field "_id")])
  (setup-field [this spec]
    (let [id-slug (str (:slug row) "_id")
          model (db/find-model (:model_id row) @field/models)]
      ((get @operations :update) :model (:model_id row)
              {:fields [{:name (util/titleize id-slug)
                         :type "integer"
                         :editable false
                         :reference :location}]} {:op :migration})
      (db/create-index (:slug model) id-slug)))

  (rename-field [this old-slug new-slug])

  (cleanup-field [this]
    (let [fields ((get @field/models (:model_id row)) :fields)
          id (keyword (str (:slug row) "_id"))]
      ((get @operations :destroy) :field (-> fields id :row :id))))

  (target-for [this] nil)
  (update-values [this content values]
    (let [posted (content (keyword (:slug row)))
          idkey (keyword (str (:slug row) "_id"))
          preexisting (content idkey)
          address (if preexisting (assoc posted :id preexisting) posted)]
      (if address
        (let [geocode (geocode-address address)
              location ((get @operations :create) :location
                        (merge address geocode))]
          (assoc values idkey (location :id)))
        values)))
  (post-update [this content opts] content)
  (pre-destroy [this content] content)

  (join-fields [this prefix opts]
    (assoc/model-select-fields (:location @field/models)
                               (str prefix "$" (:slug row)) opts))

  (join-conditions [this prefix opts]
    (let [model (@field/models (:model_id row))
          slug (:slug row)
          id-slug (keyword (str slug "_id"))
          id-field (-> model :fields id-slug)
          field-select (field/coalesce-locale model id-field prefix
                                               (name id-slug) opts)]
      [(util/clause "left outer join location %2$%1 on (%3 = %2$%1.id)"
                    [(:slug row) prefix field-select])]))

  (build-where
    [this prefix opts]
    (field/with-propagation :where opts (:slug row)
      (fn [down]
        (assoc/model-where-conditions (:location @field/models)
                                      (str prefix "$" (:slug row)) down))))

  (natural-orderings [this prefix opts])

  (build-order [this prefix opts]
    (assoc/join-order this (:location @field/models) prefix opts))

  (field-generator [this generators]
    generators)

  (fuse-field [this prefix archetype skein opts]
    (assoc/join-fusion this (:location @field/models) prefix archetype skein opts))

  (localized? [this] false)
  (models-involved [this opts all] all)

  (field-from [this content opts]
    (or (db/choose :location (content (keyword (str (:slug row) "_id")))) {}))

  (render [this content opts]
    (assoc/join-render this (:location @field/models) content opts))
  (validate [this opts] (validation/for-address this opts)))

(field/add-constructor :address (fn [row operations]
                                  (AddressField. row {} operations)))
                      