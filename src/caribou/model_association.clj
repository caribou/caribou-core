(ns caribou.model-association
  (:require [clojure.string :as string]
            [caribou.util :as util]
            [caribou.field-protocol :as field]))

(defn with-propagation
  [sign opts field includer]
  (if-let [outer (sign opts)]
    (if-let [inner (outer (keyword field))]
      (let [down (assoc opts sign inner)]
        (includer down)))))

(defn table-columns
  "Return a list of all columns for the table corresponding to this model."
  [slug]
  (let [model (@field/models (keyword slug))]
    (apply
     concat
     (map
      (fn [field]
        (map
         #(name (first %))
         (field/table-additions field (-> field :row :slug))))
      (vals (model :fields))))))

(defn present?
  [x]
  (and (not (nil? x))
       (or (number? x) (keyword? x) (= (type x) Boolean) (not (empty? x)))))

(defn model-join-conditions
  "Find all necessary table joins for this query based on the arbitrary
   nesting of the include option."
  [model prefix opts]
  (let [fields (:fields model)]
    (filter
     identity
     (apply
      concat
      (map
       (fn [field]
         (field/join-conditions field (name prefix) opts))
       (vals fields))))))

(defn assoc-field
  [content field opts]
  (assoc
    content
    (keyword (-> field :row :slug))
    (field/field-from field content opts)))

(defn from
  "takes a model and a raw db row and converts it into a full
  content representation as specified by the supplied opts.
  some opts that are supported:
    include - a nested hash of association includes.  if a key matches
    the name of an association any content associated to this item through
    that association will be inserted under that key."
  [model content opts]
  (reduce
   #(assoc-field %1 %2 opts)
   content
   (vals (model :fields))))

(defn model-natural-orderings
  "Find all orderings between included associations that depend on the association position column
   of the given model."
  [model prefix opts]
  (filter
   identity
   (flatten
    (map
     (fn [order-key]
       (if-let [field (-> model :fields order-key)]
         (field/natural-orderings
          field (name prefix)
          (assoc opts :include (-> opts :include order-key)))))
     (keys (:include opts))))))

(defn model-models-involved
  [model opts all]
  (reduce #(field/models-involved %2 opts %1) all (-> model :fields vals)))

(defn span-models-involved
  [field opts all]
  (if-let [down (with-propagation :include opts (-> field :row :slug)
                  (fn [down]
                    (let [target (@field/models (-> field :row :target_id))]
                      (model-models-involved target down all))))]
    down
    all))

(defn model-where-conditions
  "Builds the where part of the uberquery given the model, prefix and
   given map of the where conditions."
  [model prefix opts]
  (let [eyes
        (filter
         identity
         (map
          (fn [field]
            (field/build-where field prefix opts))
          (vals (:fields model))))]
    (string/join " and " (flatten eyes))))

;; the next few functions are all really helpers for model-select-fields

(defn table-fields
  "This is part of the Field protocol that is the same for all fields.
  Returns the set of fields that could play a role in the select. "
  [field]
  (concat (map first (field/table-additions field (-> field :row :slug)))
          (field/subfield-names field (-> field :row :slug))))

(defn build-alias
  [model field prefix slug opts]
  (let [select-field (field/coalesce-locale model field prefix slug opts)]
    (str select-field " as " prefix "$" (name slug))))

(defn select-fields
  "Find all necessary columns for the select query based on the given include nesting
   and fashion them into sql form."
  [model field prefix opts]
  (let [columns (table-fields field)
        next-prefix (str prefix (:slug field))
        model-fields (map #(build-alias model field prefix % opts) columns)
        join-model-fields (field/join-fields field next-prefix opts)]
    (concat model-fields join-model-fields)))

(defn model-select-fields
  "Build a set of select fields based on the given model."
  [model prefix opts]
  (let [fields (vals (:fields model))
        sf (fn [field]
             (select-fields model field (name prefix) opts))
        model-fields (map sf fields)]
    (set (apply concat model-fields))))

(defn model-build-order
  "Builds out the order component of the uberquery given whatever ordering map
   is found in opts."
  [model prefix opts]
  (filter
   identity
   (flatten
    (map
     (fn [field]
       (field/build-order field prefix opts))
     (vals (:fields model))))))

(defn model-render
  "render a piece of content according to the fields contained in the model
  and given by the supplied opts"
  [model content opts]
  (let [fields (vals (:fields model))]
    (reduce
     (fn [content field]
       (field/render field content opts))
     content fields)))

(defn subfusion
  [model prefix skein opts]
  (let [fields (vals (:fields model))
        archetype
        (reduce
         (fn [archetype field]
           (field/fuse-field field prefix archetype skein opts))
         {} fields)]
    archetype))

(defn part-fusion
  [this target prefix archetype skein opts]
  (let [slug (keyword (-> this :row :slug))
        fused
        (with-propagation :include opts slug
          (fn [down]
            (let [value (subfusion target (str prefix "$" (name slug))
                                   skein down)]
              (if (:id value)
                (assoc archetype slug value)
                archetype))))]
    (or fused archetype)))

(defn fusion
  "Takes the results of the uberquery, which could have a map for each
   item associated to a given piece of content, and fuses them into a
   single nested map representing that content."
  [model prefix fibers opts]
  (let [model-key (util/prefix-key prefix "id")
        order (distinct (map model-key fibers))
        world (group-by model-key fibers)
        fused (util/map-vals #(subfusion model prefix % opts) world)]
    (map #(fused %) order)))

(defn collection-fusion
  [this prefix archetype skein opts]
  (let [slug (keyword (-> this :row :slug))
        nesting 
        (with-propagation :include opts slug
          (fn [down]
            (let [target (@field/models (-> this :row :target_id))
                  value (fusion target (str prefix "$" (name slug)) skein down)
                  protected (filter :id value)]
              (assoc archetype slug protected))))]
    (or nesting archetype)))

(defn join-order
  [field target prefix opts]
  (let [slug (keyword (-> field :row :slug))]
    (with-propagation :order opts slug
      (fn [down]
        (model-build-order target (str prefix "$" (name slug)) down)))))

(defn join-fusion
  ([this target prefix archetype skein opts]
     (join-fusion this target prefix archetype skein opts identity))
  ([this target prefix archetype skein opts process]
     (let [slug (keyword (-> this :row :slug))
           value (subfusion target (str prefix "$" (name slug)) skein opts)]
       (if (:id value)
         (assoc archetype slug (process value))
         archetype))))

(defn join-render
  [this target content opts]
  (let [slug (keyword (-> this :row :slug))]
    (if-let [sub (slug content)]
      (update-in
       content [slug]
       (fn [part]
         (model-render target part opts)))
      content)))

(defn part-render
  [this target content opts]
  (if-let [include (:include opts)]
    (let [slug (keyword (-> this :row :slug))
          down {:include (slug include)}]
      (if-let [sub (slug content)]
        (update-in
         content [slug] 
         (fn [part]
           (model-render target part down)))
        content))
    content))
