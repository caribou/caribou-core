(ns caribou.field.link
  (:require [clojure.string :as string]
            [caribou.field :as field]
            [caribou.util :as util]
            [caribou.db :as db]
            [caribou.validation :as validation]
            [caribou.association :as assoc]))

(defn join-table-name
  "construct a join table name out of two link names"
  [a b]
  (string/join "_" (sort (map util/slugify [a b]))))

(defn link-join-name
  "Given a link field, return the join table name used by that link."
  [field]
  (let [reciprocal (-> field :env :link)
        from-name (-> field :row :name)
        to-name (:slug reciprocal)]
    (keyword (join-table-name from-name to-name))))

(defn link-keys
  "Find all related keys given by this link field."
  [field]
  (let [reciprocal (-> field :env :link)
        from-name (-> field :row :slug)
        from-key (keyword (str from-name "_id"))
        to-name (reciprocal :slug)
        to-key (keyword (str to-name "_id"))
        join-key (keyword (join-table-name from-name to-name))]
    {:from from-key :to to-key :join join-key}))

(defn link-join-keys
  [this prefix opts]
  (let [{from-key :from to-key :to join-key :join} (link-keys this)
        from-name (-> this :row :slug)
        join-model (get @field/models join-key)
        join-alias (str prefix "$" from-name "_join")
        join-field (-> join-model :fields to-key)
        link-field (-> join-model :fields from-key)
        table-alias (str prefix "$" from-name)
        join-select (field/coalesce-locale join-model join-field join-alias
                                            (name to-key) opts)
        link-select (field/coalesce-locale join-model link-field join-alias
                                            (name from-key) opts)]
    {:join-key (name join-key)
     :join-alias join-alias
     :join-select join-select
     :table-alias table-alias
     :link-select link-select}))

(defn remove-link
  ([field from-id to-id]
     (remove-link field from-id to-id {}))
  ([field from-id to-id opts]
     (let [{from-key :from to-key :to join-key :join} (link-keys field)
           locale (if (:locale opts) (str (name (:locale opts)) "_") "")
           params [join-key from-key to-id to-key from-id locale]
           preexisting (first (apply (partial util/query "select * from %1 where %6%2 = %3 and %6%4 = %5") params))]
       (if preexisting
         ((resolve 'caribou.model/destroy) join-key (preexisting :id))))))

(defn- link-join-conditions
  [field prefix opts]
  (let [slug (-> field :row :slug)]
    (assoc/with-propagation :include opts slug
      (fn [down]
        (let [{:keys [join-key join-alias join-select table-alias link-select]}
              (link-join-keys field prefix opts)
              target (@field/models (-> field :row :target_id))
              join-params [join-key join-alias join-select prefix]
              link-params [(:slug target) table-alias link-select]
              downstream (assoc/model-join-conditions target table-alias down)]
          (concat
           [(util/clause "left outer join %1 %2 on (%3 = %4.id)" join-params)
            (util/clause "left outer join %1 %2 on (%2.id = %3)" link-params)]
           downstream))))))

(defn- link-where
  [field prefix opts]
  (let [slug (-> field :row :slug)
        join-clause "%1.id in (select %2 from %3 %4 inner join %5 %8 on (%6 = %8.id) where %7)"]
    (assoc/with-propagation :where opts slug
      (fn [down]
        (let [{:keys [join-key join-alias join-select table-alias link-select]}
              (link-join-keys field prefix opts)
              model (@field/models (-> field :row :model_id))
              target (@field/models (-> field :row :target_id))
              subconditions (assoc/model-where-conditions target table-alias down)
              params [prefix join-select join-key join-alias
                      (:slug target) link-select subconditions table-alias]] 
          (util/clause join-clause params))))))

(defn- link-natural-orderings
  [field prefix opts]
  (let [slug (-> field :row :slug)
        reciprocal (-> field :env :link)
        model (@field/models (-> field :row :model_id))
        target (@field/models (-> field :row :target_id))
        to-name (reciprocal :slug)
        from-key (keyword (str slug "_position"))
        join-alias (str prefix "$" slug "_join")
        join-key (keyword (join-table-name slug to-name))
        join-model (@field/models join-key)
        join-field (-> join-model :fields from-key)
        join-select (field/coalesce-locale model join-field join-alias
                                            (name from-key) opts)
        downstream (assoc/model-natural-orderings target (str prefix "$" slug) opts)]
    [(str join-select " asc") downstream]))

(defn- link-render
  [this content opts]
  (if-let [include (:include opts)]
    (let [slug (keyword (-> this :row :slug))]
      (if-let [sub (slug include)]
        (let [target (@field/models (-> this :row :target_id))
              down {:include sub}]
          (update-in
           content [slug]
           (fn [col]
             (doall
              (map
               (fn [to]
                 (assoc/model-render target to down))
               col)))))
        content))
    content))

(defn link-rename-field
  [field old-slug new-slug]
  (let [model (get @field/models (:model_id (:row field)))
        target (get @field/models (:target_id (:row field)))
        reciprocal (-> field :env :link)
        reciprocal-slug (:slug reciprocal)
        old-join-key (keyword (str (name old-slug) "_join"))
        old-join-name (join-table-name (name old-slug) reciprocal-slug)
        new-join-key (keyword (str (name new-slug) "_join"))
        new-join-name (join-table-name (name new-slug) reciprocal-slug)
        join-model (get @field/models (keyword old-join-name))
        join-collection (-> model :fields old-join-key)
        old-key (keyword old-slug)
        join-target (-> join-model :fields old-key)]
    ((resolve 'caribou.model/update) :field (-> join-collection :row :id)
     {:name (util/titleize new-join-key) :slug (name new-join-key)})
    ((resolve 'caribou.model/update) :field (-> join-target :row :id)
     {:name (util/titleize new-slug) :slug (name new-slug)})
    ((resolve 'caribou.model/update) :model (:id join-model)
     {:name (util/titleize new-join-name) :slug new-join-name})))

(defn link-propagate-order
  [this id orderings])

(defn link-models-involved
  [field opts all]
  (if-let [down (assoc/with-propagation :include opts (-> field :row :slug)
                  (fn [down]
                    (let [slug (-> field :row :slug)
                          reciprocal (-> field :env :link)
                          to-name (reciprocal :slug)
                          join-key (keyword (join-table-name slug to-name))
                          join-id (-> @field/models join-key :id)
                          target (@field/models (-> field :row :target_id))]
                      (assoc/model-models-involved target down (conj all join-id)))))]
    down
    all))

(defn link
  "Link two rows by the given LinkField.  This function accepts its arguments
   in order, so that 'a' is a row from the model containing the given field."
  ([field a b]
     (link field a b {}))
  ([field a b opts]
     (let [{from-key :from to-key :to join-key :join} (link-keys field)
           target-id (-> field :row :target_id)
           target (or (get @field/models target-id)
                      (first (util/query "select * from model where id = %1"
                                         target-id)))
           locale (if (and (:localized target)
                           (:locale opts))
                    (str (name (:locale opts)) "_")
                    "")
           linkage ((resolve 'caribou.model/create) (:slug target) b opts)
           params [join-key from-key (:id linkage) to-key (:id a) locale]
           query "select * from %1 where %6%2 = %3 and %6%4 = %5"
           preexisting (apply (partial util/query query) params)]
       (if preexisting
         preexisting
         ((resolve 'caribou.model/create) join-key {from-key (:id linkage)
                                                    to-key (:id a)} opts)))))

(defn retrieve-links
  "Given a link field and a row, find all target rows linked to the given row
   by this field."
  ([field content]
     (retrieve-links field content {}))
  ([field content opts]
     (let [{from-key :from to-key :to join-key :join} (link-keys field)
           target (@field/models (-> field :row :target_id))
           target-slug (target :slug)
           locale (if (:locale opts) (str (name (:locale opts)) "_") "")
           field-names (map #(str target-slug "." %)
                            (assoc/table-columns target-slug))
           field-select (string/join "," field-names)
           join-query "select %1 from %2 inner join %3 on (%2.id = %3.%7%4) where %3.%7%5 = %6"
           params [field-select target-slug join-key from-key to-key (content :id) locale]]
       (apply (partial util/query join-query) params))))

(defrecord LinkField [row env]
  field/Field

  (table-additions [this field] [])
  (subfield-names [this field] [])

  (setup-field
    [this spec]
    (if (or (nil? (:link_id row)) (zero? (:link_id row)))
      (let [model (db/find-model (:model_id row) @field/models)
            target (db/find-model (:target_id row) @field/models)
            reciprocal-name (or (:reciprocal_name spec) (:name model))
            join-name (join-table-name (:name spec) reciprocal-name)

            link
            ((resolve 'caribou.model/create)
             :field
             {:name reciprocal-name
              :type "link"
              :model_id (:target_id row)
              :target_id (:model_id row)
              :link_id (:id row)
              :dependent (:dependent row)})

            join-model
            ((resolve 'caribou.model/create)
             :model
             {:name (util/titleize join-name)
              :join_model true
              :localized (or (:localized model) (:localized target))
              :fields
              [{:name (:name spec)
                :type "part"
                :dependent true
                :reciprocal_name (str reciprocal-name " Join")
                :target_id (:target_id row)}
               {:name reciprocal-name
                :type "part"
                :dependent true
                :reciprocal_name (str (:name spec) " Join")
                :target_id (:model_id row)}]} {:op :migration})]

        (db/update :field ["id = ?" (util/convert-int (:id row))]
                   {:link_id (:id link)}))))

  (rename-model [this old-slug new-slug])
  (rename-field
    [this old-slug new-slug]
    (link-rename-field this old-slug new-slug))

  (cleanup-field [this]
    (try
      (let [join-name (link-join-name this)]
        ((resolve 'caribou.model/destroy) :model
         (-> @field/models join-name :id))
        ((resolve 'caribou.model/destroy) :field (row :link_id)))
      (catch Exception e (str e))))

  (target-for [this] (@field/models (row :target_id)))

  (update-values [this content values]
    (let [removed (content (keyword (str "removed_" (:slug row))))]
      (if (assoc/present? removed)
        (let [ex (map util/convert-int (string/split removed #","))]
          (doall (map #(remove-link this (content :id) %) ex)))))
    values)

  (post-update [this content opts]
    (if-let [collection (content (keyword (:slug row)))]
      (let [linked (doall (map #(link this content % opts)
                               collection))
            with-links (assoc content (keyword (str (:slug row) "_join")) linked)]
        (assoc content (:slug row) (retrieve-links this content opts))))
    content)

  (pre-destroy [this content]
    content)

  (join-fields [this prefix opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (let [target (@field/models (:target_id row))]
          (assoc/model-select-fields target (str prefix "$" (:slug row))
                                     down)))))

  (join-conditions [this prefix opts]
    (link-join-conditions this prefix opts))

  (build-where
    [this prefix opts]
    (link-where this prefix opts))

  (natural-orderings [this prefix opts]
    (link-natural-orderings this prefix opts))

  (build-order [this prefix opts]
    (assoc/join-order this (@field/models (:target_id row)) prefix opts))

  (field-generator [this generators]
    generators)

  (fuse-field [this prefix archetype skein opts]
    (assoc/collection-fusion this prefix archetype skein opts))

  (localized? [this] false)

  (propagate-order [this id orderings]
    (link-propagate-order this id orderings))

  (models-involved [this opts all]
    (link-models-involved this opts all))

  (field-from [this content opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (let [target (field/target-for this)]
          (map
           #(assoc/from target % down)
           (retrieve-links this content opts))))))

  (render [this content opts]
    (link-render this content opts))

  (validate [this opts] (validation/for-assoc this opts)))

(defn constructor
  [row]
  (let [link (db/choose :field (row :link_id))]
    (LinkField. row {:link link})))

