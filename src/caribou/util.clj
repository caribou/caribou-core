(ns caribou.util
  (:require [clojure.string :as string]
            [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]))

(import java.util.regex.Matcher)
(import java.sql.SQLException)
(import java.io.File)

(defn convert-int
  [whatever]
  (if whatever
    (if (= (type whatever) java.lang.String)
      (try
        (Integer. whatever)
        (catch Exception e nil))
      (.intValue whatever))))

(defn seq-to-map [f q]
  (reduce #(assoc %1 (f %2) %2) {} q))

(defn slugify [s]
  (.toLowerCase (string/replace (string/join "-" (re-seq #"[a-zA-Z0-9]+" (name s))) #"^[0-9]" "-")))

(defn url-slugify [s]
  (.toLowerCase (string/join "-" (re-seq #"[a-zA-Z0-9]+" (name s)))))

(defn titleize [s]
  (string/join " " (map string/capitalize (string/split (name s) #"[^a-zA-Z]+"))))

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

(defn map-keys
  [f m]
  (into {} (for [[k v] m] [(f k) v])))

(defn map-vals
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn map-map
  [f m]
  (into {} (for [[k v] m] (f k v))))

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
    ;; this is not actually an error
    (catch Exception e (println (str "No properties file named "
                                        props-name)))))

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

;; db support -------------------------------

(def naming-strategy
  {:entity
   (fn [k]
     (string/replace (name k) "-" "_"))
   :keyword
   (fn [e]
     (keyword (string/lower-case (string/replace e "_" "-"))))})

(defn zap
  "quickly sanitize a potentially dirty string in preparation for a sql query"
  [s]
  (cond
   (string? s) (.replaceAll (re-matcher #"[\\\";#%]" (.replaceAll (str s) "'" "''")) "")
   (keyword? s) (zap (name s))
   :else s))

(defn dbize
  [s]
  (if (or (keyword? s) (string? s))
    (sql/as-named-identifier naming-strategy (keyword (zap s)))
    s))

(defn clause
  "substitute values into a string template based on numbered % parameters"
  [pred args]
  (letfn [(rep [s i] (.replaceAll s (str "%" (inc i))
                                  (let [item (nth args i)]
                                    (Matcher/quoteReplacement
                                     (cond
                                      (keyword? item) (dbize (name item))
                                      :else
                                      (str item))))))]
    (if (empty? args)
      pred
      (loop [i 0 retr pred]
        (if (= i (-> args count dec))
          (rep retr i)
          (recur (inc i) (rep retr i)))))))

(defn query
  "make an arbitrary query, substituting in extra args as % parameters"
  [q & args]
  (sql/query (config/)
    [(clause q args)]
    (doall res)))

; by Chouser:
(defn deep-merge-with
  "Like merge-with, but merges maps recursively, applying the given fn
  only when there's a non-map at a particular level.

  (deepmerge + {:a {:b {:c 1 :d {:x 1 :y 2}} :e 3} :f 4}
               {:a {:b {:c 2 :d {:z 9} :z 3} :e 100}})
  -> {:a {:b {:z 3, :c 3, :d {:z 9, :x 1, :y 2}}, :e 103}, :f 4}"
  [f & maps]
  (apply
    (fn m [& maps]
      (if (every? map? maps)
        (apply merge-with m maps)
        (apply f maps)))
    maps))

(defn prefix-key
  [prefix slug]
  (keyword (str (name prefix) "$" (name slug))))


(def pool "abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ
           =+!@#$%^&*()_-|:;?/}]{[~` 0123456789")

(defn rand-str
  ([n] (rand-str n pool))
  ([n pool]
     (string/join
      (map
       (fn [_]
         (rand-nth pool))
       (repeat n nil)))))

(defn maybe-require
  "require the given ns, ignore file not found errors, but let others
  do their thing"
  [ns]
  (try
    (require ns :reload)
    (catch java.io.FileNotFoundException e nil)))

(defn run-namespace
  [namespace action-symbol]
  (let [namespace-symbol (symbol namespace)]
    (maybe-require namespace-symbol)
    (if-let [running (find-ns namespace-symbol)]
      (if-let [action (ns-resolve running (symbol action-symbol))]
        (action)))))

