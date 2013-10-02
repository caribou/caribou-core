(ns caribou.association
  (:require [clojure.string :as string]
            [clojure.walk :as walk]
            [antlers.core :as antlers]
            [caribou.config :as config]
            [caribou.logger :as log]
            [caribou.util :as util]
            [caribou.field :as field]))

(defn with-propagation
  [sign opts field includer]
  (if-let [outer (get opts sign)]
    (if-let [inner (get outer (keyword field))]
      (let [down (assoc opts sign inner)]
        (includer down)))))

(defn table-columns
  "Return a list of all columns for the table corresponding to this model."
  [slug]
  (let [model (field/models (keyword slug))]
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

(defn seq->map
  [s key]
  (let [result (reduce
                (fn [result item]
                  (if-let [subkey (get item key)]
                    (assoc result (keyword subkey) item)
                    result))
                {} s)]
    result))

(defn model-join-conditions
  "Find all necessary table joins for this query based on the arbitrary
   nesting of the include option."
  [model prefix opts]
  (let [fields (:fields model)]
    (doall
     (filter
      identity
      (apply
       concat
       (map
        (fn [field]
          (field/join-conditions field (name prefix) opts))
        (vals fields)))))))

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
  (doall
   (filter
    identity
    (flatten
     (map
      (fn [order-key]
        (if-let [field (-> model :fields order-key)]
          (field/natural-orderings
           field (name prefix)
           (assoc opts :include (-> opts :include order-key)))))
      (keys (:include opts)))))))

(defn model-models-involved
  [model opts all]
  (reduce #(field/models-involved %2 opts %1) all (-> model :fields vals)))

(defn span-models-involved
  [field opts all]
  (if-let [down (with-propagation :include opts (-> field :row :slug)
                  (fn [down]
                    (let [target (field/models (-> field :row :target-id))]
                      (model-models-involved target down all))))]
    down
    all))

(declare model-where-conditions)

(defn model-monadic-condition
  [model prefix opts where key op]
  (let [clause (get where key)
        subopts (assoc opts :where clause)
        inner (model-where-conditions model prefix subopts)]
      (list {:op op :value inner})))

(defn model-dyadic-condition
  [model prefix opts where key op]
  (let [clauses (get where key)
        subwheres (mapcat
                   (fn [clause]
                     (model-where-conditions 
                      model prefix 
                      (assoc opts :where clause))) 
                   clauses)]
    (list {:op op :value subwheres})))

(defn model-where-conditions
  "Builds the where part of the uberquery given the model, prefix and
   given map of the where conditions."
  [model prefix opts]
  (let [where (:where opts)]
    (condp = (-> where keys first)
      :! (model-monadic-condition model prefix opts where :! "NOT")
      :|| (model-dyadic-condition model prefix opts where :|| "OR")
      :&& (model-dyadic-condition model prefix opts where :&& "AND")

      'not (model-monadic-condition model prefix opts where 'not "NOT")
      'or (model-dyadic-condition model prefix opts where 'or "OR")
      'and (model-dyadic-condition model prefix opts where 'and "AND")

      (let [eyes
            (filter
             identity
             (map
              (fn [field]
                (field/build-where field prefix opts))
              (vals (:fields model))))]
        (doall (flatten eyes))))))

(defn part-where
  [field target prefix opts]
  (let [slug (-> field :row :slug)]
    (with-propagation :where opts slug
      (fn [down]
        (let [model (field/models (-> field :row :model-id))
              part-id-slug (keyword (str slug "-id"))
              part-id-field (-> model :fields part-id-slug)
              part-select (field/coalesce-locale 
                           model part-id-field prefix (name part-id-slug) opts)
              id-field (-> target :fields :id)
              table-alias (str prefix "$" slug)
              field-select (field/coalesce-locale model id-field table-alias "id" opts)
              subconditions (model-where-conditions target table-alias down)]
          {:field part-select
           :op "in"
           :value {:select field-select
                   :from [(:slug target) table-alias]
                   :where subconditions}})))))

(defn table-fields
  "This is part of the Field protocol that is the same for all fields.
  Returns the set of fields that could play a role in the select. "
  [field]
  (let [slug (-> field :row :slug)]
    (concat
     (map first (field/table-additions field slug))
     (field/subfield-names field slug))))

(defn build-alias
  [model field prefix slug opts]
  (let [select-field (field/coalesce-locale model field prefix slug opts)]
    [select-field (str prefix "$" (name slug))]))

(defn find-subfields
  "Given a field and an options map, find any fields specified under the :fields key
  that belong to this field."
  [field opts]
  (let [slug (-> field :row :slug keyword)
        subfields (filter identity (map #(get % slug) (:fields opts)))]
    (assoc opts :fields (first subfields))))

(defn select-fields
  "Find all necessary columns for the select query based on the given include nesting
   and fashion them into sql form."
  [model field prefix opts shearing]
  (let [columns (table-fields field)
        slug (-> field :row :slug keyword)
        sheared (if shearing (filter shearing columns) columns)
        next-prefix (str prefix (:slug field))
        model-fields (map #(build-alias model field prefix % opts) sheared)
        subfields (find-subfields field opts)
        join-model-fields (when-not (and shearing (not (shearing slug)))
                            (field/join-fields field next-prefix subfields))]
    (concat model-fields join-model-fields)))

(defn extract-field-key
  [fields-item]
  (if (map? fields-item)
    (keys fields-item)
    fields-item))

(defn find-shearing
  "finds the subset of fields that should be selected based on the :fields key of the
  opts map"
  [opts]
  (when-let [retaining (:fields opts)]
    (set (flatten (map extract-field-key retaining)))))

(defn model-select-fields
  "Build a set of select fields based on the given model."
  [model prefix opts]
  (let [fields (-> model :fields vals)
        shearing (find-shearing opts)
        shearing (if shearing (conj shearing :id))
        model-fields (map #(select-fields model % (name prefix) opts shearing) fields)]
    (set (apply concat model-fields))))

(defn model-build-order
  "Builds out the order component of the uberquery given whatever ordering map
   is found in opts."
  [model prefix opts]
  (doall
   (filter
    identity
    (flatten
     (map
      (fn [field]
        (field/build-order field prefix opts))
      (vals (:fields model)))))))

(defn model-render
  "render a piece of content according to the fields contained in the model
  and given by the supplied opts"
  [model content opts]
  (let [fields (vals (:fields model))]
    (reduce
     (fn [content field]
       (field/render field content opts))
     content fields)))

(defn shear-fields
  [fields shearing]
  (if shearing 
    (filter 
     (fn [field] 
       ((conj shearing :id) (-> field :row :slug keyword)))
     fields)
    fields))

(defn subfusion
  [model prefix skein opts]
  (let [shearing (find-shearing opts)
        fields (shear-fields (-> model :fields vals) shearing)
        extra-fields (shear-fields (:extra-fields opts) shearing)
        opts (dissoc opts :extra-fields)
        archetype
        (reduce
         (fn [archetype field]
           (let [subfields (find-subfields field opts)]
             (field/fuse-field field prefix archetype skein subfields)))
         {} fields)]
    (reduce
     #(field/pure-fusion %2 prefix %1 skein opts)
     archetype
     extra-fields)))

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

(defn part-fusion
  [this target prefix archetype skein opts]
  (let [slug (keyword (-> this :row :slug))
        fused
        (with-propagation :include opts slug
          (fn [down]
            (let [value (subfusion
                         target
                         (str prefix "$" (name slug))
                         skein down)]
              (if (:id value)
                (assoc archetype slug value)
                archetype))))]
    (or fused archetype)))

(defn collection-fusion
  ([this prefix archetype skein opts]
     (let [key-slug (-> this :env :link :slug (str "-key") keyword)
           fusion-op (if (-> this :row :map)
                       #(seq->map % key-slug)
                       identity)]
       (collection-fusion this prefix archetype skein opts fusion-op)))
  ([this prefix archetype skein opts process]
     (let [slug (keyword (-> this :row :slug))
           nesting 
           (with-propagation :include opts slug
             (fn [down]
               (let [target (field/models (-> this :row :target-id))
                     value (fusion target (str prefix "$" (name slug)) skein down)
                     protected (filter :id value)]
                 (assoc archetype slug (process protected)))))]
       (or nesting archetype))))

(defn map-fusion
  ([this prefix archetype skein opts]
     (let [slug (-> this :row :slug)
           key-slug (keyword (str slug "-key"))
           position-slug (keyword (str slug "-position"))
           fusion-op (if (-> this :row :map)
                       #(seq->map % key-slug)
                       identity)
           extra-fields [{:row {:slug key-slug}} {:row {:slug position-slug}}]
           opts (assoc opts :extra-fields extra-fields)]
       (collection-fusion this prefix archetype skein opts fusion-op)))
  ([this prefix archetype skein opts process]
     (collection-fusion this prefix archetype skein opts process)))

(defn join-fusion
  ([this target prefix archetype skein opts]
     (join-fusion this target prefix archetype skein opts identity))
  ([this target prefix archetype skein opts process]
     (let [slug (keyword (-> this :row :slug))
           value (subfusion target (str prefix "$" (name slug)) skein opts)]
       (if (:id value)
         (assoc archetype slug (process value))
         archetype))))

(defn join-order
  [field target prefix opts]
  (let [slug (keyword (-> field :row :slug))]
    (with-propagation :order opts slug
      (fn [down]
        (model-build-order target (str prefix "$" (name slug)) down)))))

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

(defrecord ModelDisplay [template])

(defn model-display
  [model content]
  (if-let [display (:display-tree model)]
    (map
     (fn [item]
       (assoc item
         :model-display
         (util/postwalk
          (fn [form]
            (if (= (type form) ModelDisplay)
              (antlers/render (:template form) item)
              form))
          display)))
     content)
    content))
