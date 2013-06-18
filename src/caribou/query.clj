(ns caribou.query
  (:require [clojure.walk :as walk]
            [clojure.string :as string]
            [caribou.logger :as log]
            [caribou.util :as util]
            [caribou.config :as config]
            [caribou.db :as db]))

(def example-query
  '{:select #{[{:coalesce ["model.ancestor-id" "model.ancestor-id"]} "model$ancestor-id"]
              ["model$fields.status-id" "model$fields$status-id"]
              ["model$fields.target-id" "model$fields$target-id"]
              ["model$fields.map" "model$fields$map"]
              ["model$fields$link.link-id" "model$fields$link$link-id"]
              ["model$fields$link.immutable" "model$fields$link$immutable"]
              ["model$fields.localized" "model$fields$localized"]
              ["model$fields.default-value" "model$fields$default-value"]
              ["model.description" "model$description"]
              ["model.status-position" "model$status-position"]
              ["model$fields$link.description" "model$fields$link$description"]
              ["model$fields$link.status-position" "model$fields$link$status-position"]
              ["model$fields.dependent" "model$fields$dependent"]
              ["model$fields.immutable" "model$fields$immutable"]
              ["model$fields.link-id" "model$fields$link-id"]
              ["model$fields.description" "model$fields$description"]
              ["model$fields.status-position" "model$fields$status-position"]
              ["model.status-id" "model$status-id"]
              ["model$fields$link.status-id" "model$fields$link$status-id"]
              ["model$fields$link.target-id" "model$fields$link$target-id"]
              ["model$fields$link.map" "model$fields$link$map"]
              ["model.localized" "model$localized"]
              ["model$fields$link.localized" "model$fields$link$localized"]
              ["model$fields$link.default-value" "model$fields$link$default-value"]
              ["model$fields$link.dependent" "model$fields$link$dependent"]
              ["model$fields.updated-at" "model$fields$updated-at"]
              ["model$fields$link.type" "model$fields$link$type"]
              ["model$fields.disjoint" "model$fields$disjoint"]
              ["model$fields.required" "model$fields$required"]
              ["model$fields.id" "model$fields$id"]
              ["model$fields.model-id" "model$fields$model-id"]
              ["model$fields$link.locked" "model$fields$link$locked"]
              ["model$fields$link.searchable" "model$fields$link$searchable"]
              ["model$fields$link.singular" "model$fields$link$singular"]
              ["model.locked" "model$locked"]
              ["model.searchable" "model$searchable"]
              ["model$fields.format" "model$fields$format"]
              ["model$fields.name" "model$fields$name"]
              ["model$fields.slug" "model$fields$slug"]
              ["model$fields$link.updated-at" "model$fields$link$updated-at"]
              ["model.updated-at" "model$updated-at"]
              ["model$fields.editable" "model$fields$editable"]
              ["model.join-model" "model$join-model"]
              ["model$fields$link.disjoint" "model$fields$link$disjoint"]
              ["model$fields$link.required" "model$fields$link$required"]
              ["model$fields$link.id" "model$fields$link$id"]
              ["model$fields.position" "model$fields$position"]
              ["model$fields.model-position" "model$fields$model-position"]
              ["model$fields$link.model-id" "model$fields$link$model-id"]
              ["model.id" "model$id"]
              ["model$fields.created-at" "model$fields$created-at"]
              ["model$fields$link.name" "model$fields$link$name"]
              ["model$fields$link.slug" "model$fields$link$slug"]
              ["model$fields$link.format" "model$fields$link$format"]
              ["model.abstract" "model$abstract"]
              ["model.nested" "model$nested"]
              ["model.name" "model$name"]
              ["model.slug" "model$slug"]
              ["model$fields$link.editable" "model$fields$link$editable"]
              ["model$fields.type" "model$fields$type"]
              ["model$fields$link.position" "model$fields$link$position"]
              ["model$fields$link.model-position" "model$fields$link$model-position"]
              ["model.position" "model$position"]
              ["model$fields.searchable" "model$fields$searchable"]
              ["model$fields.locked" "model$fields$locked"]
              ["model$fields$link.created-at" "model$fields$link$created-at"]
              ["model.created-at" "model$created-at"]
              ["model$fields.singular" "model$fields$singular"]},
    :from ["model" "model"],
    :join ({:table ["field" "model$fields"],
            :on [{:max ["model$fields.model-id" "model$fields.model-id"]} "model.id"]}
           {:table ["field" "model$fields$link"],
            :on ["model$fields.link-id" "model$fields$link.id"]})
    :where ({:field "model.id",
             :op "in",
             :value {:select "*",
                     :from {:select "model.id",
                            :from "model",
                            :where ({:field "model.id",
                                     :op "in",
                                     :value {:select "model$fields.model-id",
                                             :from ["field" "model$fields"],
                                             :where ({:field "model$fields.slug",
                                                      :op "=",
                                                      :value "slug"})}}
                                    {:field "model.id",
                                     :op "in",
                                     :value {:select "model$fields.model-id",
                                             :from ["field" "model$fields"],
                                             :where ({:field "model$fields.id",
                                                      :op ">",
                                                      :value 1})}}),
                            :order ({:by "model.position", :direction :asc}),
                            :limit 5,
                            :offset 3},
                     :as "_conditions_"}}),
    :order ({:by "model.position", :direction :asc}
            {:by "model$fields.model-position", :direction :asc})})

(declare construct-query)

(defn construct-subquery
  [form params]
  (let [[subform params] (construct-query form params)]
    [(str "(" subform ")") params]))

(defn construct-select-function
  [field]
  (if (map? field)
    (let [function (first (keys field))
          args (get field function)
          arg-list (string/join ", " args)
          call (str (name function) "(" arg-list ")")]
      call)
    field))

(defn construct-select-alias
  [[field alias]]
  (let [field (construct-select-function field)]
    (str field " as " alias)))

(defn construct-select
  [select-form]
  (cond
    (vector? select-form) (construct-select-alias select-form)
    (string? select-form) select-form))

(defn construct-selects
  [select-forms]
  (let [select (cond
                 (string? select-forms) select-forms
                 (map? select-forms) (construct-select-function select-forms)
                 :else (let [as (map construct-select select-forms)]
                         (string/join ", " as)))]
    (str "select " select)))

(defn construct-from
  [from-form params]
  (let [[subform params] (cond
                           (map? from-form) (construct-subquery from-form params)
                           (vector? from-form) [(str (first from-form) " as " (last from-form)) params]
                           :else [from-form params])]
    [(str "from " subform) params]))

(defn construct-join
  [join-form]
  (if-let [{:keys [table on]} join-form]
    (let [[join alias] table
          [rhs lhs] on
          select (construct-select-function rhs)]
      (str "left outer join " join " as " alias " on (" select " = " lhs ")"))))

(defn construct-joins
  [join-forms]
  (let [joins (map construct-join join-forms)]
    (string/join " " joins)))

(defn construct-where
  [where-form params]
  (if-let [{:keys [field op value]} where-form]
    (let [[subform params] 
          (cond
           (nil? value) ["NULL" params]
           (map? value) (construct-subquery value params)
           :else ["?" (conj params value)])]
          ;; (if (map? value)
          ;;   (construct-subquery value params)
          ;;   ["?" (conj params value)])]
      [(str (construct-select-function field) " " op " " subform) params])))

(defn construct-wheres
  [where-forms params]
  (if-not (empty? where-forms)
    (let [[wheres params] (reduce
                           (fn [[wheres params] where-form]
                             (let [[subform params] (construct-where where-form params)]
                               [(conj wheres subform) params]))
                           [[] params]
                           where-forms)
          clauses (string/join " and " wheres)]
      [(str "where " clauses) params])
    ["" params]))

(defn construct-as
  [as-form]
  (if as-form
    (str "as " as-form)))

(defn construct-order
  [{:keys [by direction]}]
  (str (construct-select-function by) " " (name direction)))

(defn construct-orders
  [order-forms]
  (if order-forms
    (let [orders (map construct-order order-forms)]
      (str "order by " (string/join ", " orders)))))

(defn construct-limit-offset
  [limit-form offset-form]
  (if limit-form
    (str "limit " limit-form " offset " offset-form)))

(defn construct-query
  ([query-map] (construct-query query-map []))
  ([{:keys [select from join where as order limit offset] :as query-map} params]
     (let [select-query (construct-selects select)
           [from-query params] (construct-from from params)
           join-query (construct-joins join)
           [where-query params] (construct-wheres where params)
           as-query (construct-as as)
           order-query (construct-orders order)
           limit-offset-query (construct-limit-offset limit offset)
           query-parts [select-query from-query join-query where-query as-query order-query limit-offset-query]]
       [(string/join " " (remove empty? query-parts)) params])))

(defn execute-query
  [query-map]
  (let [[query params] (construct-query query-map)
        db-query (util/underscore query)]
    (log/out :QUERY db-query)
    (db/query db-query params)))

;; QUERY CACHE ----------------------------

(defn queries
  []
  (config/draw :query :queries))

(defn reverse-cache
  []
  (config/draw :query :reverse-cache))

(defn sort-map
  [m]
  (if (map? m)
    (sort m)
    m))

(defn walk-sort-map
  [m]
  (walk/postwalk sort-map m))

(defn hash-query
  [model-key opts]
  (str (name model-key) (walk-sort-map opts)))

(defn cache-query
  [query-hash results]
  (swap! (queries) assoc query-hash results))

(defn reverse-cache-add
  [id code]
  (let [cache (reverse-cache)]
    (if (contains? @cache id)
      (swap! cache #(update-in % [id] (fn [v] (conj v code))))
      (swap! cache #(assoc % id (list code))))))

(defn reverse-cache-get
  [id]
  (get (deref (reverse-cache)) id))

(defn reverse-cache-delete
  [ids]
  (swap! (reverse-cache) #(apply dissoc (cons % ids))))

(defn clear-model-cache
  [ids]
  (swap!
   (queries)
   #(apply
     (partial dissoc %)
     (mapcat (fn [id] (reverse-cache-get id)) ids)))
  (reverse-cache-delete ids))

(defn clear-queries
  []
  (reset! (queries) {})
  (reset! (reverse-cache) {}))

(defn retrieve-query
  [query-hash]
  (get (deref (queries)) query-hash))

;; QUERY DEFAULTS ----------------------------------------
;; this could live elsewhere
(defn expand-query-defaults
  [query-defaults clause]
  (if (not (empty? clause))
    (into query-defaults
          (for [[k v] (filter #(-> % val map?) clause)]
            [k (expand-query-defaults query-defaults v)]))
    query-defaults))

(defn expanded-query-defaults
  [query query-defaults]
  (let [expanded-include (expand-query-defaults query-defaults (:include query))
        expanded-where   (expand-query-defaults query-defaults (:where query))
        merged-defaults  (util/deep-merge-with (fn [& maps] (first maps))
                                               expanded-where expanded-include)]
    merged-defaults))

(defn apply-query-defaults
  [query query-defaults]
  (if (not= query-defaults nil)
    (util/deep-merge-with
     (fn [& maps] (first maps)) query
     {:where (expanded-query-defaults query
                                      query-defaults)})
    query))
