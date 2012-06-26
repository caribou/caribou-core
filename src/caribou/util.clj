(ns caribou.util
  (:use caribou.debug)
  (:require [clojure.string :as string]
            [clojure.java.io :as io]))

(import java.sql.SQLException)
(import java.io.File)

(defn seq-to-map [f q]
  (reduce #(assoc %1 (f %2) %2) {} q))

(defn slugify [s]
  (.toLowerCase (string/replace (string/join "_" (re-seq #"[a-zA-Z0-9]+" s)) #"^[0-9]" "_")))

(defn titleize [s]
  (string/join " " (map string/capitalize (string/split s #"[^a-zA-Z]+"))))

(def file-separator
  (str (.get (java.lang.System/getProperties) "file.separator")))

(defn pathify
  [paths]
  (string/join file-separator paths))

(defn file-exists?
  [path]
  (.exists (io/file path)))

(defn pull-resource
  [path]
  (io/resource path))

(defn map-vals
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn re-replace
  [r s f]
  (let [between (string/split s r)
        inside (re-seq r s)
        transformed (concat (map f inside) [""])]
    (apply str (interleave between transformed))))

(defn re-replace-first
  [r s f]
  (let [between (string/split s r)
        inside (re-seq r s)
        transformed (concat [(f (first inside))] (rest inside) [""])]
    (apply str (interleave between transformed))))

(defn re-replace-beginning
  [r s]
  (let [[_ after] (re-find r s)]
    after))

(defn render-exception [e]
  (if-let [cause (.getCause e)]
    (if (isa? cause SQLException)
      (let [next (.getNextException cause)]
        (str next (.printStackTrace next)))
      (str cause (.printStackTrace cause)))
    (str e (.printStackTrace e))))

(defn get-file-extension [file]
  (let [filename (.getName file)]
  (.toLowerCase (.substring filename (.lastIndexOf filename ".")))))

(defn load-props
  [props-name]
  (try 
    (let [raw (io/reader (io/resource props-name))
          props (java.util.Properties.)]
      (.load props raw)
      (into {} (for [[k v] props] [(keyword k) (read-string v)])))
    (catch Exception e (println "No properties file named" props-name))))

(defn load-resource
  [resource-name]
  (let [thr (Thread/currentThread)
        ldr (.getContextClassLoader thr)]
    (.getResourceAsStream ldr resource-name)))

(defn load-path [path visit]
  (doseq [file (file-seq (io/file path))]
    (let [filename (.toString file)
          subname (string/replace filename (str path "/") "")]
      (if (.isFile file)
        (visit file subname)))))

(defn walk
  "A version of clojure.walk/walk that doesn't think Records are Maps"
  [inner outer form]
  (cond
   (list? form) (outer (apply list (map inner form)))
   (seq? form) (outer (doall (map inner form)))
   (vector? form) (outer (vec (map inner form)))
   (and (map? form) (not (instance? clojure.lang.IRecord form))) (outer (into (if (sorted? form) (sorted-map) {})
                            (map inner form)))
   (set? form) (outer (into (if (sorted? form) (sorted-set) #{})
                            (map inner form)))
   :else (outer form)))

(defn postwalk
  "clojure.walk/postwalk that uses our walk function"
  [f form]
  (walk (partial postwalk f) f form))

(defn stringify-keys
  "A version of clojure.walk/stringify-keys that is record-aware"
  [m]
  (let [f (fn [[k v]] (if (keyword? k) [(name k) v] [k v]))]
    ;; only apply to maps (not records)
    (postwalk (fn [x] (if (and (map? x) (not (instance? clojure.lang.IRecord x))) (into {} (map f x)) x)) m)))
