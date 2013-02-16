(ns caribou.field-protocol
  (:require [caribou.db :as db]
            [caribou.util :as util]))

(defprotocol Field
  "a protocol for expected behavior of all model fields"
  (table-additions [this field]
    "the set of additions to this db table based on the given name")
  (subfield-names [this field]
    "the names of any additional fields added to the model
    by this field given this name")

  (setup-field [this spec] "further processing on creation of field")
  (rename-field [this old-slug new-slug] "further processing on creation of field")
  (cleanup-field [this] "further processing on removal of field")
  (target-for [this] "retrieves the model this field points to, if applicable")
  (update-values [this content values]
    "adds to the map of values that will be committed to the db for this row")
  (post-update [this content opts]
    "any processing that is required after the content is created/updated")
  (pre-destroy [this content]
    "prepare this content item for destruction")

  (join-fields [this prefix opts])
  (join-conditions [this prefix opts])
  (build-where [this prefix opts] "creates a where clause suitable to this field from the given where map, with fields prefixed by the given prefix.")
  (natural-orderings [this prefix opts])
  (build-order [this prefix opts])
  (field-generator [this generators])
  (fuse-field [this prefix archetype skein opts])

  (localized? [this])
  (models-involved [this opts all])

  (field-from [this content opts]
    "retrieves the value for this field from this content item")
  (render [this content opts] "renders out a single field from this content item")
  (validate [this opts] "given a set of options and the models, verifies the options are appropriate and well formed for gathering"))

;; fields use the set of defined models extensively
(def models (ref {}))

(defn build-locale-field
  [prefix slug locale]
  (str prefix "." (name locale) "_" (name slug)))

(defn build-select-field
  [prefix slug]
  (str prefix "." (name slug)))

(defn build-coalesce
  [prefix slug locale]
  (let [global (build-select-field prefix slug)
        local (build-locale-field prefix slug locale)]
    (str "coalesce(" local ", " global ")")))

(defn select-locale
  [model field prefix slug opts]
  (let [locale (:locale opts)]
    (if (and locale (:localized model) (localized? field))
      (build-locale-field prefix slug locale)
      (build-select-field prefix slug))))

(defn coalesce-locale
  [model field prefix slug opts]
  (let [locale (:locale opts)]
    (if (and locale (:localized model) (localized? field))
      (build-coalesce prefix slug locale)
      (build-select-field prefix slug))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn table-fields
  "This is part of the Field protocol that is the same for all fields.
  Returns the set of fields that could play a role in the select. "
  [field]
  (concat (map first (table-additions field (-> field :row :slug)))
          (subfield-names field (-> field :row :slug))))

(defn build-alias
  [model field prefix slug opts]
  (let [select-field (coalesce-locale model field prefix slug opts)]
    (str select-field " as " prefix "$" (name slug))))

(defn select-fields
  "Find all necessary columns for the select query based on the given include nesting
   and fashion them into sql form."
  [model field prefix opts]
  (let [columns (table-fields field)
        next-prefix (str prefix (:slug field))
        model-fields (map #(build-alias model field prefix % opts) columns)
        join-model-fields (join-fields field next-prefix opts)]
    (concat model-fields join-model-fields)))

(defn with-propagation
  [sign opts field includer]
  (if-let [outer (sign opts)]
    (if-let [inner (outer (keyword field))]
      (let [down (assoc opts sign inner)]
        (includer down)))))

(defn where-operator
  [where]
  (if (map? where)
    [(-> where keys first name) (-> where vals first)]
    ["=" where]))

(defn field-where
  [field prefix opts do-where]
  (let [slug (keyword (-> field :row :slug))
        where (-> opts :where slug)]
    (if-not (nil? where)
      (do-where field prefix slug opts where))))

(defn pure-fusion
  [this prefix archetype skein opts]
  (let [slug (keyword (-> this :row :slug))
        bit (util/prefix-key prefix slug)
        containing (drop-while #(nil? (get % bit)) skein)
        value (get (first containing) bit)]
    (assoc archetype slug value)))

;; put this in the boolean field namespace

(defn boolean-fusion
  [this prefix archetype skein opts]
  (let [slug (keyword (-> this :row :slug))
        bit (util/prefix-key prefix slug)
        containing (drop-while #(nil? (get % bit)) skein)
        value (get (first containing) bit)
        value (or (= 1 value) (= true value))]
    (assoc archetype slug value)))

;; put this in assoc-field namespace?

(defn id-models-involved
  [field opts all]
  (conj all (-> field :row :model_id)))

(defn pure-where
  [field prefix slug opts where]
  (let [model-id (-> field :row :model_id)
        model (db/find-model model-id @models)
        [operator value] (where-operator where)
        field-select (coalesce-locale model field prefix slug opts)]
    (util/clause "%1 %2 %3" [field-select operator value])))

(defn pure-order
  [field prefix opts]
  (let [slug (-> field :row :slug)]
    (if-let [by (get (:order opts) (keyword slug))]
      (let [model-id (-> field :row :model_id)
            model (db/find-model model-id @models)]
        (str (coalesce-locale model field prefix slug opts) " "
             (name by))))))

(defn string-where
  [field prefix slug opts where]
  (let [model-id (-> field :row :model_id)
        model (db/find-model model-id @models)
        [operator value] (where-operator where)
        field-select (coalesce-locale model field prefix slug opts)]
    (util/clause "%1 %2 '%3'" [field-select operator value])))

(def field-constructors
  (atom {}))

(defn add-constructor
  [key construct]
  (swap! field-constructors (fn [c] (assoc c key construct))))
