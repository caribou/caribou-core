(ns caribou.model-association
  (:require [clojure.string :as string]
            [caribou.field-protocol :as field]))


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

(defn model-select-fields
  "Build a set of select fields based on the given model."
  [model prefix opts]
  (let [fields (vals (:fields model))
        sf (fn [field]
             (field/select-fields model field (name prefix) opts))
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

(defn join-order
  [field target prefix opts]
  (let [slug (keyword (-> field :row :slug))]
    (field/with-propagation :order opts slug
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

