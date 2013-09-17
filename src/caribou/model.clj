(ns caribou.model
  (:require [clojure.string :as string]
            [clojure.walk :as walk]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as sql]
            [pantomime.mime :as mime]
            [caribou.util :as util]
            [caribou.config :as config]
            [caribou.logger :as log]
            [caribou.asset :as asset]
            [caribou.db :as db]
            [caribou.hooks :as hooks]
            [caribou.field :as field]
            [caribou.field.constructors :as field-constructors]
            [caribou.field.timestamp :as timestamp]
            [caribou.field.link :as link]
            [caribou.query :as query]
            [caribou.validation :as validation]
            [caribou.index :as index]
            [caribou.association :as association]))

(def current-timestamp timestamp/current-timestamp)
(def retrieve-links link/retrieve-links)
(def present? association/present?)
(def from association/from)
(def link link/link)

;; FIELD INTEGRATION -----------------

(def models field/models)

(defn make-field
  "turn a row from the field table into a full fledged Field record"
  [row]
  (let [type (:type row)
        constructor (field/get-constructor type)]
    (when-not constructor
      (throw (new Exception (str "no such field type: " type))))
    (constructor row)))

(def base-fields
  [{:name "Id" :slug "id" :type "id" :locked true :immutable true
    :editable false}
   {:name "UUID" :slug "uuid" :type "string" :locked true :immutable true :editable false}
   {:name "Position" :slug "position" :type "position" :locked true}
   {:name "Env Id" :slug "env-id" :type "integer" :locked true :editable false}
   {:name "Locked" :slug "locked" :type "boolean" :locked true :immutable true
    :editable false :default-value false}
   {:name "Created At" :slug "created-at" :type "timestamp" 
    :locked true :immutable true :editable false}
   {:name "Updated At" :slug "updated-at" :type "timestamp" :locked true
    :editable false}])

(defn add-base-fields
  [content]
  (log/debug (str "Adding base fields to model with id" (:id content)))
  (doseq [field base-fields]
    (db/insert
     :field
     (merge
      field
      {:model-id (:id content)
       :updated-at (current-timestamp)}
      (if (contains? (:fields (models :field)) :uuid)
        {:uuid (util/random-uuid)} {})))))

;; UBERQUERY ---------------------------------------------

(defn model-slugs
  []
  (filter keyword? (keys (models))))

(declare update destroy create)

(defn model-select-query
  "Build the select query for this model by the given prefix based on the
   particular nesting of the include map."
  [model prefix opts]
  (let [selects (association/model-select-fields model prefix opts)
        joins (association/model-join-conditions model prefix opts)]
    {:select selects
     :from [(:slug model) prefix]
     :join joins}))

(defn model-limit-offset
  "Determine the limit and offset component of the uberquery based on
  the given where condition."
  [limit offset]
  {:limit limit :offset offset})

(defn immediate-vals
  [m]
  (into
   {}
   (filter
    (fn [[k v]]
      (not
       (or (map? v)
           (seq? v)
           (vector? v)
           (set? v))))
    m)))

(defn model-order-statement
  "Joins the natural orderings and the given orderings from the opts
   map and constructs the order clause to ultimately be used in the
   uberquery."
  [model opts]
  (let [ordering (if (empty? (:order opts)) 
                   (assoc-in opts [:order :position] :asc)
                   opts)
        order (association/model-build-order model (:slug model) ordering)]
    order))

(defn table-for
  [column]
  (let [column (if (map? column) (first (first (vals column))) column)]
    (first (string/split column #"\."))))

(defn find-order-tables
  [orders]
  (reduce
   (fn [tables order]
     (let [table (table-for (:by order))]
       (assoc tables table order)))
   {} orders))

(defn from-table
  [from]
  (if (sequential? from)
    (last from) from))

(declare merge-select-tree)

(defn merge-where-tree
  [order-tables where]
  (let [value (:value where)]
    (if (and (map? value) (contains? value :from))
      (update-in where [:value] (partial merge-select-tree order-tables))
      where)))

(defn merge-select-tree
  [order-tables select]
  (let [select-table (from-table (:from select))
        order-for-table (get order-tables select-table)
        merged-wheres (update-in select [:where] #(map (partial merge-where-tree order-tables) %))]
    (if order-for-table
      (update-in merged-wheres [:order] #(concat % (list order-for-table)))
      merged-wheres)))

(defn merge-order-inner
  [order inner]
  (let [order-tables (find-order-tables order)]
    (merge-select-tree order-tables inner)))

(defn model-outer-condition
  [model where order limit-offset opts]
  (let [model-id (str (:slug model) ".id")
        inner (merge
               {:select model-id
                :from (:slug model)
                :where where}
               limit-offset)
        merged (merge-order-inner order inner)]
    (list
     {:field model-id
      :op "in"
      :value {:select "*"
              :from merged
              :as "_conditions_"}})))

(defn form-uberquery
  "Given the model and map of opts, construct the corresponding
  uberquery (but don't call it!)"
  [model opts]
  (let [query (model-select-query model (:slug model) opts)
        where (association/model-where-conditions model (:slug model) opts)

        order (model-order-statement model opts)
        natural (association/model-natural-orderings model (:slug model) opts)
        final-order (concat order natural)

        limit-offset (when-let [limit (:limit opts)]
                       (model-limit-offset limit (or (:offset opts) 0)))
        condition (model-outer-condition model where final-order limit-offset opts)]
    (assoc query
      :where condition
      :order final-order)))

(defn construct-uberquery
  [model opts]
  (let [form (form-uberquery model opts)]
    (query/construct-query form)))

(defn uberquery
  "The query to bind all queries.  Returns every facet of every row given an
   arbitrary nesting of include relationships (also known as the uberjoin)."
  [model opts]
  (let [query-mass (form-uberquery model opts)]
    (query/execute-query query-mass)))

(defn beam-splitter
  "Splits the given options (:include, :where, :order) out into
   parallel paths to avoid übercombinatoric explosion!  Returns a list
   of options each of which correspond to an independent set of
   includes."
  [opts]
  (if-let [include-keys (-> opts :include keys)]
    (let [keys-difference (fn [a b]
                            (set/difference (-> a keys set) (-> b keys set)))
          split-keys [:include :order]
          unsplit (apply (partial dissoc opts) split-keys)
          reduce-split
          (fn [include-key split key]
            (let [split-opts (if-let [inner (-> opts key include-key)]
                               (assoc split key {include-key inner})
                               split)
                  subopts (get opts key)
                  other-keys (keys-difference subopts (:include opts))
                  merge-subopts-other
                  (fn [a]
                    (merge a (select-keys subopts other-keys)))
                  ultimate (update-in split-opts [key] merge-subopts-other)]
              (merge ultimate unsplit)))]
      (map (fn [include-key]
             (reduce (partial reduce-split include-key)
                     {} split-keys))
           include-keys))
    [opts]))

(defn gather
  "The main function to retrieve instances of a model given by the
  slug.

   The possible keys in the opts map are:

     :include - a nested map of associated content to include with the
     found instances.

     :where - a nested map of conditions given to constrain the
     results found, which could include associated content.

     :order - a nested map of ordering statements which may or may not
     be across associations.

     :limit - The number of primary results to return (the number of
              associated instances given by the :include option are
              not limited).

     :offset - how many records into the result set are returned.

   Example: (gather :model {:include {:fields {:link {}}}
                            :where {:fields {:slug \"name\"}}
                            :order {:slug :asc}
                            :limit 10
                            :offset 3})

     --> returns 10 models and all their associated fields (and those
         fields' links if they exist) who have a field with the slug
         of 'name', ordered by the model slug and offset by 3."
  ([slug] (gather slug {}))
  ([slug opts]
     (let [query-defaults (config/draw :query :query-defaults)
           ;; defaulted (query/apply-query-defaults opts query-defaults)
           defaulted opts
           query-hash (query/hash-query slug defaulted)]
       (if-let [cached (and (config/draw :query :enable-query-cache)
                            (query/retrieve-query query-hash))]
         cached
         (let [model (models (keyword slug))]
           (when-not model
             (throw (new Exception (str "invalid caribou model:" slug))))
           ;; beam-validator throws an exception if opts are bad
           ;; (when (and (seq opts) (not (= (config/environment) :production)))
           ;;   (try
           ;;     (validation/beams slug opts)
           ;;     (catch Exception e
           ;;       (.printStackTrace e))))
           (let [beams (beam-splitter defaulted)
                 resurrected (mapcat (partial uberquery model) beams)
                 fused (association/fusion model (name slug) resurrected defaulted)
                 involved (association/model-models-involved model defaulted #{})]
             (query/cache-query query-hash fused)
             (doseq [m involved]
               (query/reverse-cache-add m query-hash))
             fused))))))

(defn pick
  "pick is the same as gather, but returns only the first result, so is
  not a list of maps but a single map result."
  ([slug] (pick slug {}))
  ([slug opts]
     (first (gather slug (assoc opts :limit 1)))))

(defn impose
  "impose is identical to pick except that if the record with the given
   :where conditions is not found, it is created according to that
   :where map."
  [slug opts]
  (or
   (pick slug opts)
   (create slug (:where opts))))

(defn previous
  ([slug item order] (previous slug item order {}))
  ([slug item order opts]
     (let [order (keyword order)
           opts (assoc-in opts [:where order :<] (get item order))
           opts (assoc-in opts [:order order] :desc)]
       (pick slug opts))))

(defn following
  ([slug item order] (following slug item order {}))
  ([slug item order opts]
     (let [order (keyword order)
           opts (assoc-in opts [:where order :>] (get item order))
           opts (assoc-in opts [:order order] :asc)]
       (pick slug opts))))

(defn total
  ([slug] (total slug {}))
  ([slug opts]
     ;; probably a better way to do this (involving select fields etc...)
     (count (gather slug opts))))

(defn translate-directive
  "Used to decompose strings into the nested maps required by the uberquery."
  [directive find-path]
  (if (and directive (not (empty? directive)))
    (let [clauses (string/split directive #",")
          paths (map find-path clauses)
          ubermap
          (reduce
           (fn [directive [path condition]]
             (update-in directive path (fn [_] condition)))
           {} paths)]
      ubermap)
    {}))

(defn process-include
  "The :include option is parsed into a nested map suitable for calling
  by the uberquery.

  It translates strings of the form:
     'association.further-association,other-association'

   into --> {:association {:further-association {}} :other-association
     {}}"
  [include]
  (translate-directive
   include
   (fn [include]
     [(map keyword (string/split include #"\.")) {}])))

(defn process-where
  "The :where option is parsed into a nested map suitable for calling
  by the uberquery.

   It translates strings of the form: 'fields.slug=name'

   into --> {:fields {:slug \"name\"}"
  [where]
  (translate-directive
   where
   (fn [where]
     (let [[path condition] (string/split where #":")]
       [(map keyword (string/split path #"\.")) condition]))))

(defn process-order
  "The :order option is parsed into a nested map suitable for calling
  by the uberquery.

   It translates strings of the form: 'fields.slug asc,position desc'
     into --> {:fields {:slug :asc} :position :desc}"
  [order]
  (translate-directive
   order
   (fn [order]
     (let [[path condition] (string/split order #" ")]
       [(map keyword (string/split path #"\."))
        (keyword (or condition "asc"))]))))

(defn find-all
  "This function is the same as gather, but uses strings for all the
   options that are transformed into the nested maps that gather
   requires.  See the various process-* functions in this same
   namespace."
  [slug opts]
  (let [include (process-include (:include opts))
        where (process-where (:where opts))
        order (process-order (:order opts))]
    (gather slug (merge opts {:include include :where where :order order}))))

(defn find-one
  "This is the same as find-all, but returns only a single item, not a vector."
  [slug opts]
  (first (find-all slug opts)))

;; HOOKS ---------------------------------------------------------

(defn model-hooks-ns
  [base slug]
  (symbol (str base "." (name slug))))

(defn add-app-model-hooks
  "finds and loads every namespace under (config/draw :hooks :namespace) that matches
   the name of a model and runs the function 'add-hooks in that namespace."
  []
  (if-let [hooks-ns (config/draw :hooks :namespace)]
    (let [make-hook-ns (partial model-hooks-ns hooks-ns)]
      (doseq [hook-namespace (map make-hook-ns (model-slugs))]
        (util/run-namespace hook-namespace 'add-hooks)))))

(defn add-parent-id
  [env]
  (if (and (-> env :content :nested) (not (-> env :original :nested)))
    (create
     :field
     {:name "Parent Id" :model-id (-> env :content :id) :type "integer"}))
  env)

;; LOCALIZATION --------------------------

(defn localized?
  [field]
  (and
   (field/localized? field)
   (-> field :row :localized)))

(defn localize-values
  [model values opts]
  (let [locale (util/zap (:locale opts))]
    (if locale
      (util/map-map
       (fn [k v]
         (let [kk (keyword k)
               field (-> model :fields kk)]
           (if (localized? field)
             [(keyword (str locale "-" (name k))) v]
             [k v])))
       values)
      values)))

(defn localized-slug
  [code slug]
  (keyword (str code "-" (name slug))))

(defn localize-field
  [model-slug field locale]
  (let [local-slug (localized-slug (:code locale) (-> field :row :slug))]
    (doseq [additions (field/table-additions field local-slug)]
      (db/add-column model-slug (name (first additions)) (rest additions)))))

(defn localize-field-for-all-locales
  [model-slug field]
  (doseq [locale (gather :locale)]
    (localize-field model-slug field locale)))

(defn localize-model
  [model]
  (doseq [field (filter #(-> % :row :localized) (-> model :fields vals))]
    (localize-field-for-all-locales (:slug model) field)))

(defn local-models
  []
  (map last (filter (fn [[k v]] (keyword? k)) (models))))

(defn add-locale
  [locale]
  (doseq [model (local-models)]
    (doseq [field (filter #(-> % :row :localized) (-> model :fields vals))]
      (localize-field (:slug model) field locale))))

(defn update-locale
  [old-code new-code]
  (doseq [model (local-models)]
    (doseq [field (filter localized? (-> model :fields vals))]
      (let [field-slug (-> field :row :slug)
            old-slug (localized-slug old-code field-slug)
            new-slug (localized-slug new-code field-slug)]
        (db/rename-column (:slug model) old-slug new-slug)))))

(defn add-status-to-model [model]
  (update :model (:id model) {:fields [{:name "Status"
                                        :type "part"
                                        :locked true
                                        :target-id (models :status :id)
                                        :reciprocal-name (:name model)}]})
  (let [status-id-field (pick :field {:where {:name "Status Id"
                                              :model-id (:id model)}})]
    (update :field (:id status-id-field) {:default-value 1})))

(defn add-status-part
  [env]
  (let [model-id (-> env :content :id)
        model-sans-status (pick :model {:where {:id model-id}})]
    (add-status-to-model model-sans-status))
  env)

(defn add-localization
  [env]
  (if (and (-> env :content :localized) (not (-> env :original :localized)))
    (let [slug (-> env :content :slug keyword)]
      (localize-model (models slug))))
  env)

(defn propagate-new-locale
  [env]
  (add-locale (:content env))
  env)

(defn rename-updated-locale
  [env]
  (let [old-code (-> env :original :code)
        new-code (-> env :content :code)]
    (if (not= old-code new-code)
      (update-locale old-code new-code)))
  env)

(declare invoke-models)

(defn model-after-save
  [env]
  (add-parent-id env)
  (invoke-models)
  (add-localization env)
  env)

(defn create-model-table
  "create a table with the given name."
  [model-name]
  (db/create-table
   (keyword model-name)
   [:id "SERIAL" "PRIMARY KEY"]
   [:uuid "varchar(255)"]
   [:position :integer "DEFAULT 0"]
   [:env-id :integer "DEFAULT 1"]
   [:locked :boolean "DEFAULT false"]
   [:created-at "timestamp" "NOT NULL" "DEFAULT current_timestamp"]
   [:updated-at "timestamp" "NOT NULL"])
  (db/create-index model-name "id")
  (db/create-index model-name "uuid"))

(defn create-model-table-from-name
  [model-name]
  (create-model-table 
   ((util/slug-transform util/dbslug-transform-map) model-name)))

(defn- add-model-hooks []
  (hooks/add-hook
   :model :before-create :build-table
   (fn [env]
     (create-model-table-from-name (-> env :spec :name))
     ;; (create-model-table ((util/slug-transform util/dbslug-transform-map) (-> env :spec :name)))
     env))

  (hooks/add-hook
   :model :after-create :add-base-fields
   (fn [env] 
     (add-base-fields (:content env))
     env))

  (hooks/add-hook
   :model :after-update :rename
   (fn [env]
     (let [original (-> env :original :slug)
           slug (-> env :content :slug)
           model (models (keyword original))]
       (when (not= original slug)
         (db/rename-table original slug)
         (doseq [field (-> model :fields vals)]
           (field/rename-model field original slug))))
     env))

  (hooks/add-hook
   :model :after-save :invoke-all
   (fn [env]
     (model-after-save env)
     env))

  (hooks/add-hook
   :model :after-destroy :cleanup
   (fn [env]
     (db/drop-table (-> env :content :slug))
     (invoke-models)
     env))

  ;; enable indexing for all non-system models
  ;; TODO make locale-aware
  ;; (map (fn [m]
  ;;       (when-not (:locked m)
  ;;         (hooks/add-hook (:slug m) :after-save :index
  ;;                   (fn [env]
  ;;                     (log/info (str "Indexing " (:slug m) " " (-> env :content :slug)))
  ;;                     (index/update (get models (:slug m)) (-> env :content))
  ;;                     env))))
  ;;                       (gather :model))

  (if (models :locale)
    (do
      (hooks/add-hook
       :locale :after-create :add-to-localized-models
       (fn [env]
         (propagate-new-locale env)))

      (hooks/add-hook
       :locale :after-update :rename-localized-fields
       (fn [env]
         (rename-updated-locale env)))))

  (if (models :asset)
    (do
      (hooks/add-hook
       :asset :before-save :scry-asset
       (fn [env] (asset/asset-scry-asset env)))
      (hooks/add-hook
       :asset :after-save :commit-asset
       (fn [env] (asset/asset-commit-asset env)))))

  (if (models :status)
    (hooks/add-hook
     :model :after-create :add-status-part
     (fn [env] (add-status-part env)))))


;; MORE FIELD FUNCTIONALITY --------------------------

(defn process-default
  [field-type default]
  (if (and (= "boolean" field-type) (string? default))
    (= "true" default)
    default))

(defn add-db-columns-for-field
  [field model-slug slug]
  (doseq [addition (field/table-additions field slug)]
    (if-not (= slug "id")
      (db/add-column
       model-slug
       (name (first addition))
       (rest addition)))))

(defn prepare-db-field
  [field model-slug slug content]
  (let [default (process-default (:type content) (:default-value content))]
    (if (:localized content)
      (localize-field-for-all-locales model-slug field))
    (if (present? default)
      (db/set-default model-slug slug default))
    (if (present? (:reference content))
      (do
        (db/create-index model-slug slug)
        ;; (db/add-reference model-slug slug (:reference content)
        ;;                   (if (:dependent content)
        ;;                     :destroy
        ;;                     :default))))
        ))
    (if (:required content)
      (db/set-required model-slug slug true))
    (if (:disjoint content)
      (db/set-unique model-slug slug true))))

(defn- field-add-columns
  [env]
  (let [field (make-field (:content env))
        model-id (-> env :content :model-id)
        model (db/find-model model-id (models))
        model-slug (:slug model)
        slug (-> env :content :slug)
        default (process-default (-> env :spec :type)
                                 (-> env :spec :default-value))
        reference (-> env :spec :reference)]

    (add-db-columns-for-field field model-slug slug)
    (field/setup-field field (env :spec))
    (prepare-db-field field model-slug slug (:content env))

    env))

(defn- field-reify-column
  [env]
  (let [field (make-field (env :content))
        model-id (-> field :row :model-id)
        model (db/choose :model model-id)
        model-slug (:slug model)
        model-fields (get (models model-id) :fields)
        local-field? (and (field/localized? field) (-> field :row :localized))
        locales (if local-field? (map :code (gather :locale)))
        
        original (:original env)
        content (:content env)
        
        oslug (:slug original)
        slug (:slug content)

        olocalized (:localized original)
        localized (:localized content)

        default (process-default (:type content) (:default-value content))
        required (:required content)
        unique (:unique content)
        
        spawn (apply zipmap (map #(field/subfield-names field %) [oslug slug]))
        transition (apply zipmap (map (fn [slugs]
                                        (map first
                                             (field/table-additions field
                                                                    slugs)))
                                      [oslug slug]))]

    (if (not (= oslug slug))
      (do
        (doseq [[old-name new-name] spawn]
          (let [field-id (-> (get model-fields (keyword old-name)) :row :id)]
            (update :field field-id {:name new-name
                                     :slug ((util/slug-transform util/dbslug-transform-map) new-name)})))

        (doseq [[old-name new-name] transition]
          (db/rename-column model-slug old-name new-name)
          (if local-field?
            (doseq [code locales]
              (db/rename-column model-slug (str code "-" (name old-name))
                                (str code "-" (name new-name))))))

        (field/rename-field field oslug slug)))

    (if (and localized (not= olocalized localized))
      (localize-field-for-all-locales model-slug field))

    (if (and (present? default) (not= (:default-value original) default))
      (db/set-default model-slug slug default))

    (if (and (present? required) (not= required (:required original)))
      (db/set-required model-slug slug required))

    (if (and (present? unique) (not= unique (:unique original)))
      (db/set-unique model-slug slug unique)))

  env)

(defn field-check-link-slug
  [env]
  (assoc env
    :values 
    (if (-> env :spec :link-slug)
      (let [model-id (-> env :spec :model-id)
            link-slug (-> env :spec :link-slug)
            fetch (db/fetch
                   :field
                   "model_id = ? and slug = ?"
                   model-id link-slug)
            linked (first fetch)]
        (assoc (:values env) :link-id (:id linked)))
      (:values env))))

(defn- add-field-hooks []
  (hooks/add-hook
   :field :before-save :check-link-slug
   (fn [env] (field-check-link-slug env)))
  (hooks/add-hook
   :field :after-create :add-columns
   (fn [env] (field-add-columns env)))
  (hooks/add-hook
   :field :after-update :reify-field
   (fn [env] (field-reify-column env)))
  (hooks/add-hook
   :field :after-destroy :drop-columns (fn [env]
                                         (try                                                  
                                           (if-let [content (:content env)]
                                             (let [field (make-field content)]
                                               (field/cleanup-field field)))
                                           (catch Exception e (log/render-exception e)))
                                         env)))


;; MODELS --------------------------------------------------------------------

(defn invoke-model
  "translates a row from the model table into a nested hash with
  references to its fields in a hash with keys being the field slugs
  and vals being the field invoked as a Field protocol record."
  [model]
  (let [fields (db/query
                "select * from field where model_id = ?"
                [(get model :id)])
        field-map (util/seq-to-map
                   #(keyword (-> % :row :slug))
                   (map make-field fields))]
    (hooks/make-lifecycle-hooks (:slug model))
    (assoc model :fields field-map)))

(defn add-app-fields
  "When {:field {:namespace $CONFIG}} is defined, run the function add-fields
   in that namespace"
  []
  (when-let [fields-ns (config/draw :field :namespace)]
    (util/run-namespace fields-ns 'add-fields)))

(defn invoke-fields
  []
  (add-app-fields)
  (doseq [[key construct] (seq field-constructors/base-constructors)]
    (field/add-constructor key construct)))

(defn bind-models
  [resurrected config]
  (reset! (:models config) resurrected))

(defn invoke-models
  "call to populate the application model cache in model/models.
  (otherwise we hit the db all the time with model and field selects)
  this also means if a model or field is changed in any way that model
  will have to be reinvoked to reflect the current state."
  []
  (invoke-fields)
  (try 
    (let [rows (db/query "select * from model")
          invoked (doall (map invoke-model rows))
          by-slug (util/seq-to-map #(-> % :slug keyword) invoked)
          by-id (util/seq-to-map :id invoked)]
      (add-model-hooks)
      (add-field-hooks)
      (bind-models (merge by-slug by-id) config/config)
      (add-app-model-hooks))
    (catch Exception e
      (log/out :INVOKE_MODELS "No models table yet!")
      (log/print-exception e))))

(defn update-values-reduction
  [spec original]
  (fn [values field]
    (field/update-values field spec values original)))

(defn create
  "slug represents the model to be updated.  the spec contains all
  information about how to update this row, including nested specs
  which update across associations.  the only difference between a
  create and an update is if an id is supplied, hence this will
  automatically forward to update if it finds an id in the spec.  this
  means you can use this create method to create or update something,
  using the presence or absence of an id to signal which operation
  gets triggered."
  ([slug spec]
     (create slug spec {}))
  ([slug spec opts]
     (if (present? (:id spec))
       (update slug (:id spec) spec opts)
       (let [model (models (keyword slug))
             values (reduce
                     (update-values-reduction spec {})
                     {} (vals (dissoc (:fields model) :updated-at)))
             env {:model model :values values :spec spec :op :create :opts opts}

             _save (hooks/run-hook slug :before-save env)
             _create (hooks/run-hook slug :before-create _save)

             local-values (localize-values model (:values _create) opts)

             uuid-values (if (and
                              (contains? (:fields model) :uuid)
                              (not (contains? local-values :uuid)))
                           (assoc local-values :uuid (util/random-uuid))
                           local-values)

             fresh (db/insert slug (assoc uuid-values
                                     :updated-at (current-timestamp)))
             content (pick 
                      slug 
                      (merge 
                       {:where {:id (:id fresh)}}
                       (if (contains? opts :locale) 
                         {:locale (:locale opts)} {})))

             indexed (index/add model content {:locale (:locale opts)})

             merged (merge (:spec _create) content)

             _after (hooks/run-hook
                     slug :after-create
                     (merge _create {:content merged}))

             post (reduce
                   #(field/post-update %2 %1 opts)
                   (:content _after)
                   (vals (:fields model)))

             _final (hooks/run-hook slug :after-save (merge _after {:content post}))]
         (query/clear-model-cache (list (:id model)))
         (:content _final)))))

(defn update
  "slug represents the model to be updated.

  id is the specific row to update.

  the spec contains all information about how to update this row,
  including nested specs which update across associations."
  ([slug id spec]
     (update slug id spec {}))
  ([slug id spec opts]
     (let [model (models (keyword slug))
           original (db/choose slug id)
           values (reduce #(field/update-values %2 (assoc spec :id id) %1 original)
                          {} (vals (model :fields)))
           env {:model model :values values :spec spec :original original
                :op :update :opts opts}
           _save (hooks/run-hook slug :before-save env)
           _update (hooks/run-hook slug :before-update _save)
           protect-uuid (dissoc (:values _update) :uuid)
           local-values (localize-values model protect-uuid opts)
           success (db/update
                    slug ["id = ?" (util/convert-int id)]
                    (assoc local-values
                      :updated-at (current-timestamp)))
           content (pick slug (merge {:where {:id id}}
                                     (if (contains? opts :locale) {:locale (:locale opts)} {})))
           indexed (index/update model content {:locale (:locale opts)})
           merged (merge (_update :spec) content)
           _after (hooks/run-hook slug :after-update
                                  (merge _update {:content merged}))
           post (reduce #(field/post-update %2 %1 opts)
                        (_after :content) (vals (model :fields)))
           _final (hooks/run-hook slug :after-save (merge _after {:content post}))]
       (query/clear-model-cache (list (:id model)))
       (_final :content))))

(defn destroy
  "destroy the item of the given model with the given id."
  [slug id]
  (let [model (models (keyword slug))
        content (db/choose slug id)
        env {:model model :content content :slug slug :op :destroy}
        _before (hooks/run-hook slug :before-destroy env)
        pre (reduce #(field/pre-destroy %2 %1)
                    (_before :content) (-> model :fields vals))
        deleted (db/delete slug "id = ?" (field/integer-conversion id))
        _ (index/delete model content)
        _after (hooks/run-hook slug :after-destroy (merge _before {:content pre}))]
    (query/clear-model-cache (list (:id model)))
    (_after :content)))

(defn order
  ([slug orderings]
     (doseq [ordering orderings]
       (update slug (:id ordering) (dissoc ordering :id))))
  ([slug id field-slug orderings]
     (let [model (models (keyword slug))
           field (-> model :fields (get (keyword field-slug)))]
       (field/propagate-order field id orderings))))

(defn- progenitors
  "if the model given by slug is nested, return a list of the item
  given by this id along with all of its ancestors."
  ([slug id] (progenitors slug id {}))
  ([slug id opts]
     (let [model (models (keyword slug))]
       (if (model :nested)
         (let [field-names (association/table-columns slug)
               base-where (util/clause "id = %1" [id])
               recur-where (util/clause "%1_tree.parent_id = %1.id" [slug])
               before (db/recursive-query slug field-names base-where
                                          recur-where)]
           (doall (map #(association/from model % opts) before)))
         [(association/from model (db/choose slug id) opts)]))))

(defn- descendents
  "pull up all the descendents of the item given by id in the nested
  model given by slug."
  ([slug id] (descendents slug id {}))
  ([slug id opts]
     (let [model (models (keyword slug))]
       (if (model :nested)
         (let [field-names (association/table-columns slug)
               base-where (util/clause "id = %1" [id])
               recur-where (util/clause "%1_tree.id = %1.parent_id" [slug])
               before (db/recursive-query slug field-names base-where
                                          recur-where)]
           (doall (map #(association/from model % opts) before)))
         [(association/from model (db/choose slug id) opts)]))))

(defn reconstruct
  "mapping is between parent-ids and collections which share a parent-id.
  node is the item whose descendent tree is to be reconstructed."
  [mapping node]
  (if (:id node)
    (assoc node :children
           (doall (map #(reconstruct mapping %) (mapping (node :id)))))))

(defn arrange-tree
  "given a set of nested items, arrange them into a tree
  based on id/parent-id relationships."
  [items]
  (if (empty? items)
    items
    (let [by-parent (group-by #(% :parent-id) items)
          roots (by-parent nil)]
      (doall
       (filter
        identity
        (map
         #(reconstruct by-parent %)
         roots))))))

;; (defn- rally
;;   "Pull a set of content up through the model system with the given
;;   options.

;;    Avoids the uberquery so is considered deprecated and inferior, left
;;    here for historical reasons (and as a hedge in case the uberquery
;;    really does explode someday!)"
;;   ([slug] (rally slug {}))
;;   ([slug opts]
;;      (let [model (field/models (keyword slug))
;;            order (or (opts :order) "asc")
;;            order-by (or (opts :order-by) "position")
;;            limit (str (or (opts :limit) 30))
;;            offset (str (or (opts :offset) 0))
;;            where (str (or (opts :where) "1=1"))
;;            query-str (string/join " "
;;                                   ["select * from %1 where %2 order by %3 %4"
;;                                    "limit %5 offset %6"])]
;;        (doall (map #(association/from model % opts)
;;                    (util/query query-str slug
;;                                where order-by order limit offset))))))

(gen-class
 :name caribou.model.Model
 :prefix model-
 :state state
 :init init
 :constructors {[String] []}
 :methods [[slug [] String]
           [create [clojure.lang.APersistentMap] clojure.lang.APersistentMap]])

(defn model-init [slug]
  [[] slug])

(defn model-create [this spec]
  (sql/with-connection (config/draw :database)
    (create (.state this) spec)))

(defn model-slug [this]
  (.state this))

(defn init
  []
  (if (nil? (config/draw :app :use-database))
    (throw (Exception. "You must set :use-database in the app config")))

  (if (empty? (config/draw :database))
    (throw (Exception. "Please configure caribou prior to initializing model")))

  (sql/with-connection (config/draw :database)
    (invoke-models)))

;; MODEL GENERATION -------------------
;; this could go in its own namespace

(defn model-generator
  "Constructs a map of field generator functions for the given model
  and its fields."
  [model]
  (let [fields (vals (:fields model))]
    (reduce #(field/field-generator %2 %1) {} fields)))

(defn generate
  "Given a map of field generator functions, create a new map that has
   a value in each key given by the field generator for that key."
  [basis]
  (loop [basis (seq basis)
         spawn {}]
    (if basis
      (let [[key well] (first basis)]
        (recur
         (next basis)
         (assoc spawn key (well))))
      spawn)))

(defn generate-model
  "Given a slug and a number n, generate that number of instances of
  the model given by that slug."
  [slug n]
  (let [g (model-generator (models slug))]
    (map (fn [_] (generate g)) (repeat n nil))))

(defn spawn-model
  "Given a slug and a number n, actually create the given number of
   model instances in the db given by the field generators for that
   model."
  [slug n]
  (let [generated (generate-model slug n)]
    (doall (map #(create slug %) generated))))

(defmacro with-models
  [config & body]
  `(db/with-db ~config
     ~@body))
