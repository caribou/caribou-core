(ns caribou.validation
  (:require [caribou.field :as field]))

(defn recursive-merge
  [a b]
  (merge-with recursive-merge a b))

(defn beams
  [slug opts]
  "verify that the given options make sense for the model given by the slug"
  (let [model (get @field/models (keyword slug))]
    (when-not model (throw (new Exception
                                (format "no such model: %s" slug))))
    (doseq [include (recursive-merge (get opts :where) (get opts :include))]
      (let [[included
             payload] include
            included-field (get (:fields model) included)
            new-opts (and payload {:where payload})]
        (when-not included-field
          (throw (new Exception (str "field '"
                                     included
                                     "' not found in model "
                                     (:slug model)))))
        (and new-opts
             (field/validate included-field new-opts))
        true))))

(defn throw-not-link
  [field-type opts]
  (throw (new Exception
              (str "field of type " field-type
                   " is not a link type so cannot be included."
                   " As requested in opts: " opts))))

(defn for-type
  [this opts predicate type-string]
  (when (seq (:include opts)) (throw-not-link type-string opts))
  (let [condition (:where opts)]
    (when (not (some
                identity
                (map apply
                     [predicate
                      (fn [s]
                        (and (string? s)
                             (predicate (read-string s))))]
                     (repeat [condition]))))
      (if (map? (:where opts))
        (throw-not-link type-string opts)
        (throw (new Exception
                    (str "cannot make a " type-string " from " condition))))))
  true)

(defn for-asset
  [this opts]
  true)

(defn for-address
  [this opts]
  true)

(defn for-associated-id
  [this id-field opts]
  (let [slug (-> this :row id-field (@field/models) :slug)]
    (beams slug opts)))

;; (defn for-part
;;   [this opts models]
;;   (for-associated-id this :model_id opts models))

(defn for-assoc
  [this opts]
  (for-associated-id this :target_id opts))

