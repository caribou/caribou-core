(ns caribou.model
  (:require [clojure.string :as string]
            [clojure.walk :as walk]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as sql]
            [caribou.util :as util]
            [caribou.config :as config]
            [caribou.logger :as log]
            [caribou.db :as db]
            [caribou.field :as field]
            [caribou.field.constructors :as field-constructors]
            [caribou.field.timestamp :as timestamp]
            [caribou.field.link :as link]
            [caribou.query :as query]
            [caribou.validation :as validation]
            [caribou.association :as association]))

(defn db
  "Calls f in the connect of the current configured database connection."
  [f]
  (db/call f))

;; COMPATIBILITY ----------------
;; stuff that is here because it is proably still being used by somebody
(def current-timestamp timestamp/current-timestamp)
(def retrieve-links link/retrieve-links)
(def present? association/present?)
(def from association/from)
(def link link/link)

(defn rally
  "Pull a set of content up through the model system with the given
  options.

   Avoids the uberquery so is considered deprecated and inferior, left
   here for historical reasons (and as a hedge in case the uberquery
   really does explode someday!)"
  ([slug] (rally slug {}))
  ([slug opts]
     (let [model (@field/models (keyword slug))
           order (or (opts :order) "asc")
           order-by (or (opts :order_by) "position")
           limit (str (or (opts :limit) 30))
           offset (str (or (opts :offset) 0))
           where (str (or (opts :where) "1=1"))
           query-str (string/join " "
                                  ["select * from %1 where %2 order by %3 %4"
                                   "limit %5 offset %6"])]
       (doall (map #(association/from model % opts)
                   (util/query query-str slug
                               where order-by order limit offset))))))

;; FIELD INTEGRATION -----------------

(def models field/models)

(defn make-field
  "turn a row from the field table into a full fledged Field record"
  [row]
  (let [type (keyword (row :type))
        constructor (@field/field-constructors type)]
    (when-not constructor
      (throw (new Exception (str "no such field type: " type))))
    (constructor row)))

(def base-fields
  [{:name "Id" :slug "id" :type "id" :locked true :immutable true
    :editable false}
   {:name "Position" :slug "position" :type "position" :locked true}
   {:name "Env Id" :slug "env_id" :type "integer" :locked true :editable false}
   {:name "Locked" :slug "locked" :type "boolean" :locked true :immutable true
    :editable false :default_value false}
   {:name "Created At" :slug "created_at" :type "timestamp"
    :default_value "current_timestamp" :locked true :immutable true
    :editable false}
   {:name "Updated At" :slug "updated_at" :type "timestamp" :locked true
    :editable false}])

(defn add-base-fields
  [env]
  (log/debug (str "Adding base fields to model with id" (-> env :content :id)))
  (doseq [field base-fields]
    (db/insert
     :field
     (merge
      field
      {:model_id (-> env :content :id)
       :updated_at (current-timestamp)})))
  env)


;; UBERQUERY ---------------------------------------------

(def model-slugs (ref {}))


(declare update destroy create)

(defn model-select-query
  "Build the select query for this model by the given prefix based on the
   particular nesting of the include map."
  [model prefix opts]
  (let [selects (string/join ", " (association/model-select-fields model prefix opts))
        joins (string/join " " (association/model-join-conditions model prefix opts))]
    (string/join " "
                 ["select" selects "from" (:slug model) (name prefix) joins])))

(defn model-limit-offset
  "Determine the limit and offset component of the uberquery based on
  the given where condition."
  [limit offset]
  (util/clause " limit %1 offset %2" [limit offset]))

(defn finalize-order-statement
  [orders]
  (let [statement (string/join ", " orders)]
    (if-not (empty? statement)
      (util/clause " order by %1" [statement]))))

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
  (let [ordering (if (:order opts) opts (assoc opts :order {:position :asc}))
        order (association/model-build-order model (:slug model) ordering)]
    (finalize-order-statement order)))

(defn model-outer-condition
  [model inner order limit opts]
  (let [subcondition (if (not (empty? inner)) (str " where " inner))
        query-string (string/join " " [" where %1.id in"
                                       "(select * from (select %1.id"
                                       "from %1 %2%3%4) as _conditions_)"])]
    (util/clause query-string
                 [(:slug model) subcondition order limit])))

(defn form-uberquery
  "Given the model and map of opts, construct the corresponding
  uberquery (but don't call it!)"
  [model opts]
  (let [query (model-select-query model (:slug model) opts)
        where (association/model-where-conditions model (:slug model) opts)

        order (model-order-statement model opts)
        
        natural (association/model-natural-orderings model (:slug model) opts)
        immediate-order (immediate-vals (:order opts))
        base-opts (if (empty? immediate-order) {} {:order immediate-order})
        base-order (model-order-statement model base-opts)
        final-order (if (empty? order)
                      (finalize-order-statement natural)
                      (string/join ", " (cons order natural)))
        limit-offset (when-let [limit (:limit opts)]
                       (model-limit-offset limit (or (:offset opts) 0)))
        condition (model-outer-condition model where base-order limit-offset
                                         opts)]
    (str query condition final-order)))

(defn uberquery
  "The query to bind all queries.  Returns every facet of every row given an
   arbitrary nesting of include relationships (also known as the uberjoin)."
  [model opts]
  (let [query-mass (form-uberquery model opts)]
    (util/query query-mass)))

(defn beam-splitter
  "Splits the given options (:include, :where, :order) out into
   parallel paths to avoid übercombinatoric explosion!  Returns a list
   of options each of which correspond to an independent set of
   includes."
  [opts]
  (if-let [include-keys (-> opts :include keys)]
    (let [keys-difference (fn   [a b]
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
     (let [query-defaults (:query-defaults @config/app)
           defaulted (query/apply-query-defaults opts query-defaults)
           query-hash (query/hash-query slug defaulted)
           cache @query/queries]
       (if-let [cached (and (:enable-query-cache @config/app)
                            (get @query/queries query-hash))]
         cached
         (let [model ((keyword slug) @models)]
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
     'association.further_association,other_association'

   into --> {:association {:further_association {}} :other_association
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
  ;; (let [oneify (assoc opts :limit 1)]
  (first (find-all slug opts)))

;; HOOKS -------------------------------------------------------

(defn add-app-model-hooks
  "finds and loads every namespace under the hooks-ns that matches the
  app's model names

  we are only loading the namespaces for their side effects, it is
  assumed that in each namespace hooks are set via add-hook as defined
  below"
  []
  (let [hooks-ns (@config/app :hooks-ns)
        make-hook-ns (fn [slug] (symbol (str hooks-ns "." (name slug))))]
    (when hooks-ns
      (doseq [model-slug @model-slugs]
        (-> model-slug make-hook-ns util/sloppy-require)))))

(def lifecycle-hooks (ref {}))

(defn make-lifecycle-hooks
  "establish the set of functions which are called throughout the
  lifecycle of all rows for a given model (slug).  the possible hook
  points are:

    :before_create -- called for create only, before the record is
    made

    :after_create -- called for create only, now the record has an id

    :before_update -- called for update only, before any changes are
    made

    :after_update -- called for update only, now the changes have been
    committed

    :before_save -- called for create and update

    :after_save -- called for create and update

    :before_destroy -- only called on destruction, record has not yet
    been removed

    :after_destroy -- only called on destruction, now the db has no
    record of it"
  [slug]
  (if (not (@lifecycle-hooks (keyword slug)))
    (let [hooks {(keyword slug)
                 {:before_create  (ref {})
                  :after_create   (ref {})
                  :before_update  (ref {})
                  :after_update   (ref {})
                  :before_save    (ref {})
                  :after_save     (ref {})
                  :before_destroy (ref {})
                  :after_destroy  (ref {})}}]
      (dosync
       (alter lifecycle-hooks merge hooks)))))

(defn run-hook
  "run the hooks for the given model slug given by timing.  env
  contains any necessary additional information for the running of the
  hook"
  [slug timing env]
  (let [kind (@lifecycle-hooks (keyword slug))]
    (if kind
      (let [hook (deref (kind (keyword timing)))]
        (reduce #((hook %2) %1) env (keys hook)))
      env)))

(defn add-hook
  "add a hook for the given model slug for the given timing.  each hook
  must have a unique id, or it overwrites the previous hook at that
  id."
  [slug timings id func]
  (let [timings (if (keyword? timings) [timings] timings)]
    (doseq [timing timings]
      (if-let [model-hooks (lifecycle-hooks (keyword slug))]
        (if-let [hook (model-hooks (keyword timing))]
          (let [hook-name (keyword id)]
            (dosync
             (alter hook merge {hook-name func})))
          (throw (Exception. (format "No model lifecycle hook called %s"
                                     timing))))
        (throw (Exception. (format "No model called %s" slug)))))))

(defn add-parent-id
  [env]
  (if (and (-> env :content :nested) (not (-> env :original :nested)))
    (create
     :field
     {:name "Parent Id" :model_id (-> env :content :id) :type "integer"}))
  env)

;; LOCALIZATION --------------------------
;; could be in its own ns?

(defn localize-values
  [model values opts]
  (let [locale (util/zap (:locale opts))]
    (if (and locale (:localized model))
      (util/map-map
       (fn [k v]
         (let [kk (keyword k)
               field (-> model :fields kk)]
           (if (field/localized? field)
             [(str locale "_" (name k)) v]
             [k v])))
       values)
      values)))

(defn localized-slug
  [code slug]
  (keyword (str code "_" (name slug))))

(defn localize-field
  [model-slug field locale]
  (let [local-slug (localized-slug (:code locale) (-> field :row :slug))]
    (doseq [additions (field/table-additions field local-slug)]
      (db/add-column model-slug (name (first additions)) (rest additions)))))

(defn localize-model
  [model]
  (doseq [field (filter field/localized? (-> model :fields vals))]
    (doseq [locale (gather :locale)]
      (localize-field (:slug model) field locale))))

(defn local-models
  []
  (filter :localized (map #(-> @models %) @model-slugs)))

(defn add-locale
  [locale]
  (doseq [model (local-models)]
    (doseq [field (filter field/localized? (-> model :fields vals))]
      (localize-field (:slug model) field locale))))

(defn update-locale
  [old-code new-code]
  (doseq [model (local-models)]
    (doseq [field (filter field/localized? (-> model :fields vals))]
      (let [field-slug (-> field :row :slug)
            old-slug (localized-slug old-code field-slug)
            new-slug (localized-slug new-code field-slug)]
        (db/rename-column (:slug model) old-slug new-slug)))))

(defn add-status-to-model [model]
  (update :model (:id model) {:fields [{:name "Status"
                                        :type "part"
                                        :target_id (-> @models :status :id)
                                        :reciprocal_name (:name model)}]})
  (let [status-id-field (pick :field {:where {:name "Status Id"
                                              :model_id (:id model)}})]
    (update :field (:id status-id-field) {:default_value 1})))

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
      (localize-model (get @models slug))))
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
   [:position :integer "DEFAULT 0"]
   [:env_id :integer "DEFAULT 1"]
   [:locked :boolean "DEFAULT false"]
   [:created_at "timestamp" "NOT NULL" "DEFAULT current_timestamp"]
   [:updated_at "timestamp" "NOT NULL"])
  (db/create-index model-name "id"))

(defn- add-model-hooks []
  (add-hook
   :model :before_create :build_table
   (fn [env]
     (create-model-table (util/slugify (-> env :spec :name)))
     env))
  
  (add-hook
   :model :after_create :add_base_fields
   (fn [env] (add-base-fields env)))
  ;; (assoc-in env [:spec :fields] (concat (-> env :spec :fields) base-fields))))

  ;; (add-hook :model :before_save :write_migrations (fn [env]
  ;;   (try                                                 
  ;;     (if (and (not (-> env :spec :locked)) (not (= (-> env :opts :op) :migration)))
  ;;       (let [now (.getTime (Date.))
  ;;             code (str "(use 'caribou.model)\n\n(defn migrate []\n  ("
  ;;                       (name (-> env :op)) " :model " (list 'quote (-> env :spec)) " {:op :migration}))\n(migrate)\n")]
  ;;         (with-open [w (io/writer (str "app/migrations/migration-" now ".clj"))]
  ;;           (.write w code))))
  ;;     (catch Exception e env))
  ;;   env))

  (add-hook
   :model :after_update :rename
   (fn [env]
     (let [original (-> env :original :slug)
           slug (-> env :content :slug)
           model (get @models (keyword original))]
       (when (not= original slug)
         (db/rename-table original slug)
         (doseq [field (-> model :fields vals)]
           (field/rename-model field original slug))))
     env))

  (add-hook
   :model :after_save :invoke_all
   (fn [env]
     (model-after-save env) ;; (invoke-models)
     env))

  ;; (add-hook :model :before_destroy :cleanup-fields (fn [env]
  ;;   (let [model (get @models (-> env :content :slug))]
  ;;     (doseq [field (-> model :fields vals)]
  ;;       (destroy :field (-> field :row :id))))
  ;;   env))

  (add-hook
   :model :after_destroy :cleanup
   (fn [env]
     (db/drop-table (-> env :content :slug))
     (invoke-models)
     env))

  (if (:locale @models)
    (do
      (add-hook
       :locale :after_create :add_to_localized_models
       (fn [env]
         (propagate-new-locale env)))

      (add-hook
       :locale :after_update :rename_localized_fields
       (fn [env]
         (rename-updated-locale env)))))

  (if (:status @models)
    (add-hook
     :model :after_create :add_status_part
     (fn [env] (add-status-part env)))))


;; MORE FIELD FUNCTIONALITY --------------------------

(defn process-default
  [field-type default]
  (if (and (= "boolean" field-type) (string? default))
    (= "true" default)
    default))

(defn- field-add-columns
  [env]
  (let [field (make-field (:content env))
        model-id (-> env :content :model_id)
        model (db/find-model model-id @models)
        model-slug (:slug model)
        slug (-> env :content :slug)
        default (process-default (-> env :spec :type)
                                 (-> env :spec :default_value))
        reference (-> env :spec :reference)]

    (doseq [addition (field/table-additions field slug)]
      (if-not (= slug "id")
        (db/add-column
         model-slug
         (name (first addition))
         (rest addition))))
    (field/setup-field field (env :spec))

    (if (present? default)
      (db/set-default model-slug slug default))
    (if (present? reference)
      (do
        (db/create-index model-slug slug)
        (db/add-reference model-slug slug reference
                          (if (-> env :content :dependent)
                            :destroy
                            :default))))
    (if (-> env :spec :required)
      (db/set-required model-slug slug true))
    (if (-> env :spec :disjoint)
      (db/set-unique model-slug slug true))

    env))

(defn- field-reify-column
  [env]
  (let [field (make-field (env :content))
        model-id (-> field :row :model_id)
        model (db/choose :model model-id)
        model-slug (:slug model)
        model-fields (get (get @models model-id) :fields)
        local-field? (and (:localized model) (field/localized? field))
        locales (if local-field? (map :code (gather :locale)))
        
        original (:original env)
        content (:content env)
        
        oslug (:slug original)
        slug (:slug content)

        default (process-default (:type content) (:default_value content))
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
                                     :slug (util/slugify new-name)})))

        (doseq [[old-name new-name] transition]
          (db/rename-column model-slug old-name new-name)
          (if local-field?
            (doseq [code locales]
              (db/rename-column model-slug (str code "_" (name old-name))
                                (str code "_" (name new-name))))))

        (field/rename-field field oslug slug)))

    (if (and (present? default) (not (= (:default_value original) default)))
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
    (if (-> env :spec :link_slug)
      (let [model_id (-> env :spec :model_id)
            link_slug (-> env :spec :link_slug)
            fetch (db/fetch :field "model_id = %1 and slug = '%2'"
                            model_id link_slug)
            linked (first fetch)]
        (assoc (:values env) :link_id (:id linked)))
      (:values env))))

(defn- add-field-hooks []
  (add-hook
   :field :before_save :check_link_slug
   (fn [env] (field-check-link-slug env)))
  (add-hook
   :field :after_create :add_columns
   (fn [env] (field-add-columns env)))
  (add-hook
   :field :after_update :reify_field
   (fn [env] (field-reify-column env)))
  (add-hook
   :field :after_destroy :drop_columns (fn [env]
    (try                                                  
      (if-let [content (:content env)]
        (let [field (make-field content)]
          (field/cleanup-field field)))
          ;; (doseq [addition (field/table-additions field (-> env :content :slug))]
          ;;   (db/drop-column (:slug model) (first addition)))))
      ;; (let [model (get @models (-> env :content :model_id))
      ;;       fields (get model :fields)
      ;;       field (fields (keyword (-> env :content :slug)))]
      ;;   (field/cleanup-field field)
      ;;   (doall (map #(db/drop-column ((models (-> field :row :model_id)) :slug) (first %)) (field/table-additions field (-> env :content :slug))))
      ;;  env)
      (catch Exception e (util/render-exception e)))
    env)))

;; MODELS --------------------------------------------------------------------

(defn invoke-model
  "translates a row from the model table into a nested hash with
  references to its fields in a hash with keys being the field slugs
  and vals being the field invoked as a Field protocol record."
  [model]
  (let [fields (util/query "select * from field where model_id = %1"
                           (get model :id))
        field-map (util/seq-to-map #(keyword (-> % :row :slug))
                                   (map make-field fields))]
    (make-lifecycle-hooks (model :slug))
    (assoc model :fields field-map)))

(defn add-app-fields
  []
  (when-let [fields-ns (@config/app :fields-ns)]
    (util/sloppy-require (symbol fields-ns))))

(defn invoke-fields
  []
  (add-app-fields)
  (doseq [[key construct] (seq field-constructors/base-constructors)]
    (field/add-constructor key construct)))

(defn invoke-models
  "call to populate the application model cache in model/models.
  (otherwise we hit the db all the time with model and field selects)
  this also means if a model or field is changed in any way that model
  will have to be reinvoked to reflect the current state."
  []
  (invoke-fields)
  (let [rows (util/query "select * from model")
        invoked (doall (map invoke-model rows))]
    (add-model-hooks)
    (add-field-hooks)
    (dosync
     (alter models 
            (fn [in-ref new-models] new-models)
            (merge (util/seq-to-map #(keyword (% :slug)) invoked)
                   (util/seq-to-map #(% :id) invoked)))))

  ;; get all of our model slugs
  (dosync
   (ref-set model-slugs (filter keyword? (keys @models))))
  (add-app-model-hooks))

(defn update-values-reduction
  [spec]
  (fn [values field]
    (field/update-values field spec values)))

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
             values (reduce (update-values-reduction spec)
                            {} (vals (dissoc (:fields model) :updated_at)))
             env {:model model :values values :spec spec :op :create :opts opts}
             _save (run-hook slug :before_save env)
             _create (run-hook slug :before_create _save)
             local-values (localize-values model (:values _create) opts)
             content (db/insert slug (assoc local-values
                                       :updated_at
                                       (current-timestamp)))
             merged (merge (:spec _create) content)
             _after (run-hook slug :after_create
                              (merge _create {:content merged}))
             post (reduce #(field/post-update %2 %1 opts)
                          (:content _after)
                          (vals (:fields model)))
             _final (run-hook slug :after_save (merge _after {:content post}))]
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
           values (reduce #(field/update-values %2 (assoc spec :id id) %1)
                          {} (vals (model :fields)))
           env {:model model :values values :spec spec :original original
                :op :update :opts opts}
           _save (run-hook slug :before_save env)
           _update (run-hook slug :before_update _save)
           local-values (localize-values model (:values _update) opts)
           success (db/update
                    slug ["id = ?" (util/convert-int id)]
                    (assoc local-values
                      :updated_at (current-timestamp)))
           content (db/choose slug id)
           merged (merge (_update :spec) content)
           _after (run-hook slug :after_update
                            (merge _update {:content merged}))
           post (reduce #(field/post-update %2 %1 opts)
                        (_after :content) (vals (model :fields)))
           _final (run-hook slug :after_save (merge _after {:content post}))]
       (query/clear-model-cache (list (:id model)))
       (_final :content))))

(defn destroy
  "destroy the item of the given model with the given id."
  [slug id]
  (let [model (get @models (keyword slug))
        content (db/choose slug id)
        env {:model model :content content :slug slug :op :destroy}
        _before (run-hook slug :before_destroy env)
        pre (reduce #(field/pre-destroy %2 %1)
                    (_before :content) (-> model :fields vals))
        deleted (db/delete slug "id = %1" id)
        _after (run-hook slug :after_destroy (merge _before {:content pre}))]
    (query/clear-model-cache (list (:id model)))
    (_after :content)))

(defn order
  ([slug orderings]
     (doseq [ordering orderings]
       (update slug (:id ordering) (dissoc ordering :id))))
  ([slug id field-slug orderings]
     (let [model (get @models (keyword slug))
           field (-> model :fields (get (keyword field-slug)))]
       (field/propagate-order field id orderings))))

(defn progenitors
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

(defn descendents
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
  "mapping is between parent_ids and collections which share a parent_id.
  node is the item whose descendent tree is to be reconstructed."
  [mapping node]
  (if (:id node)
    (assoc node :children
           (doall (map #(reconstruct mapping %) (mapping (node :id)))))))

(defn arrange-tree
  "given a set of nested items, arrange them into a tree
  based on id/parent_id relationships."
  [items]
  (if (empty? items)
    items
    (let [by-parent (group-by #(% :parent_id) items)
          roots (by-parent nil)]
      (doall
       (filter
        identity
        (map
         #(reconstruct by-parent %)
         roots))))))

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
  (sql/with-connection @config/db
    (create (.state this) spec)))

(defn model-slug [this]
  (.state this))

(defn init
  []
  (if (nil? (@config/app :use-database))
    (throw (Exception. "You must set :use-database in the app config")))

  (if (empty? @config/db)
    (throw (Exception. "Please configure caribou prior to initializing model")))

  (sql/with-connection @config/db
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
  (let [g (model-generator (slug @models))]
    (map (fn [_] (generate g)) (repeat n nil))))

(defn spawn-model
  "Given a slug and a number n, actually create the given number of
   model instances in the db given by the field generators for that
   model."
  [slug n]
  (let [generated (generate-model slug n)]
    (doall (map #(create slug %) generated))))
