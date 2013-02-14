(ns caribou.validation
  (:use caribou.field-protocol))


(defn beams

  ([slug opts models]
     "verify that the given options make sense for the model given by the slug"
     (let [model (get models (keyword slug))]
       (when-not model (throw (new Exception
                                   (format "no such model: %s" slug))))
       (beams slug opts models model)))

  ([slug opts models model]
     (println "validating" slug opts)
     (let [where-conditions (get opts :where)
           include-conditions (get opts :include)]
       (doseq [where where-conditions]
         (when-not (-> where first keyword ((:fields model)))
           (throw (new Exception (str "no such field " (first where)
                                      " in model " slug)))))
       (doseq [include include-conditions]
         (let [included (keyword (first include))
               included-field (get (:fields model) included)
               next-model (and included-field
                               (target-model included-field models))
               _ (when-not next-model
                   (throw (new Exception (str "nested include field '"
                                              (:slug included-field)
                                              "' is not an association,"
                                              " so cannot be included"))))
               new-include (second include)
               new-where (or (get where-conditions included)
                             (get where-conditions (name included)))
               new-opts {:include new-include}
               new-opts (if new-where
                          (assoc new-opts :where new-where)
                          new-opts)]
           (beams (:slug next-model) new-opts models next-model))))))

(defn atomic-target
  [this models]
  false)

(defn part-target
  [this models]
  (-> this :row :model_id models))

(defn assoc-target
  [this models]
  (-> this :row :target_id models))

(def tie-target assoc-target)

(def link-target assoc-target)

(def collection-target assoc-target)
