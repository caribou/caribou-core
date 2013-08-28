(ns caribou.io
  (:require [clojure.java.io :as io]
            [caribou.model :as model]))

(defn compound-models
  ([models] (compound-models models {}))
  ([models opts]
     (reduce 
      (fn [export model]
        (let [key (keyword model)
              all (model/gather key (get opts key))]
          (assoc export key all)))
      {} models)))

(defn ensure-directory
  [target]
  (let [file (io/file target)
        parent (.getParentFile file)]
    (if-not (.exists parent)
      (.mkdirs parent))))

(defn export-models
  ([models opts target])
  ([models opts target pre]
     (ensure-directory target)
     (let [all (compound-models models opts)
           processed (pre all)]
       (with-open [out (io/writer target)]
         (binding [*out* out]
           (pr processed))))))

(defn export-schema
  ([] (export-schema "export/schema.clj"))
  ([target]
     (export-models 
      [:model] 
      {:model 
       {:where {:locked false} 
        :include {:fields {}}}} 
      "export/schema.clj"
      (fn [schema]
        (update-in 
         schema [:model]
         (fn [models]
           (map 
            (fn [model]
              (update-in 
               model [:fields]
               (fn [fields]
                 (remove :locked fields))))
            models)))))))

(defn import-models
  [target]
  (with-open [in (java.io.PushbackReader. (io/reader target))]
    (let [all (read in)]
      all)))
