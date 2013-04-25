(ns caribou.index
  (:require [clucy.core :as clucy]
            [caribou.logger :as log]
            [fs.core :as fs]
            [caribou.config :as config]))

(def _index (ref nil))
(def required-keys [:id])

(defn index-path []
  (or (config/draw :index :path) "clucy-index"))

(defn index []
  (if-not (nil? @_index)
    @_index
    (dosync
      (ref-set _index (clucy/disk-index (index-path))))))

(defn- searchable-fields [model]
  (let [field-defs (map :row (vals (-> model :fields)))
        searchable (filter :searchable field-defs)
        unique (set searchable)]
    (seq unique)))

(defn searchable? [model]
  (> (count (searchable-fields model)) 0))

(defn- searchable-keys [model]
  (map #(-> % :slug keyword) (searchable-fields model)))

(defn default-limit [] (or (config/draw :index :default-limit) 1000))

(defn compound-key [content locale]
  (if (empty? locale)
    (:id content)
    (str (:id content) "/" locale)))

(defn prepare-for-index
  "Given a model and a content map, this returns another content
  map containing all the keys that should be either stored or
  indexed, plus meta-data indicating whether they should be stored
  or indexed, plus any required fields, plus a bucket :_indexed
  field containing all indexed content for this record."
  [model content opts]
  (let [searchable-keys (searchable-keys model)
        index-keys (if (map? (:index opts)) (keys (:index opts)) (or (:index opts) []))
        store-keys (if (map? (:store opts)) (keys (:store opts)) (or (:store opts) []))
        omit-keys  (if (map? (:omit opts)) (keys (:omit opts)) (or (:omit opts) []))
        index-key-map (into {} (map #(vector % {}) (concat index-keys searchable-keys required-keys)))
        store-key-map (into {} (map #(vector % {}) (concat store-keys searchable-keys required-keys)))
        omit-key-map (into {} (map #(vector % {}) omit-keys))
        meta-goop (into {} (map (fn [k] (vector k
                   {:stored (and (contains? store-key-map k) (not (contains? omit-key-map k)))
                    :indexed (contains? index-key-map k)
                   })) (keys content)))
        ;_ (println meta-goop)
        indexed (select-keys content (keys index-key-map))
        raw (select-keys content (keys (merge index-key-map store-key-map)))
        typed (assoc raw :caribou-model (:slug model) :_indexed (apply str (interpose " " (vals indexed))))
        local (if-let [locale (:locale opts)]
                (assoc typed :caribou-locale locale :caribou-id (compound-key typed locale))
                (assoc typed :caribou-id (:id typed)))
        tweaked (with-meta local (assoc meta-goop :_indexed {:stored false :indexed true}))]
    tweaked))

(defn identifier
  "The minimum search term required to locate a piece of content."
  [typed opts]
  (when-let [id (:id typed)]
    (str "id:" id " AND caribou-model:" (:caribou-model typed))))

(defn delete
  "Removes a piece of content from the index.  Only the
  content's id and model are relevant in this process; all
  other fields are ignored when performing the deletion.
  However, the prepared content is returned so that it may
  be used by its caller.  Note that it ignores locale."
  ([model content]
    (delete model content {}))
  ([model content opts]
    (let [typed (prepare-for-index model content opts)
          term (identifier typed opts)
          ;_ (println "purging matches to " term)
          removed (when-not (nil? term) (clucy/search-and-delete (index) term))]
      typed)))

(defn update
  "Adds or updates content in the index.  It always attempts
  to remove the content before adding it."
  ([model content]
    (update model content {}))
  ([model content opts]
    (if (searchable? model)
      (let [typed (delete model content opts)
            _ (log/debug typed)
            indexed (clucy/add (index) typed)]
        indexed)
      nil)))

(defn add
  "Adds content to the index."
  ([model content]
    (add model content {}))
  ([model content opts]
    (when (searchable? model)
      (let [typed (prepare-for-index model content opts)
          indexed (clucy/add (index) typed)]
        indexed))))

(defn search
  "Searches for content in the index.  Requires a model
  and a where clause, which is a string in a form that the
  Lucene query parser handle.  A limit can be specified,
  otherwise it defaults to `default-limit`."
  ([model where]
    (search model where {}))
  ([model where opts]
    (let [wherever (if-let [locale (:locale opts)]
                     (str where " AND caribou-locale:" locale)
                     where)]
      (clucy/search (index) wherever (or (:limit opts) (default-limit))
                        :default-field :_indexed
                        :page (or (:page opts) 0)
                        :results-per-page (or (:size opts) (default-limit))))))

(defn add-all
  "Use this when bootstrapping your index."
  ([model]
    (add-all model {}))
  ([model opts]
    (let [gather (resolve 'caribou.model/gather)]
      (when (searchable? model)
        (map #(add model % opts) (gather (:slug model) opts))))))

(defn update-all
  "Use this to reindex all content of a given model."
  ([model]
    (update-all model {}))
  ([model opts]
    (let [gather (resolve 'caribou.model/gather)]
      (when (searchable? model)
        (map #(update model % opts) (gather (:slug model) opts))))))

(defn purge
  "Deletes the index if it exists"
  []
  (when-let [dir (index-path)]
    (fs/delete-dir dir)))
