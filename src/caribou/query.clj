(ns caribou.query
  (:require [clojure.walk :as walk]
            [caribou.util :as util]))

(def queries (atom {}))
(def reverse-cache (atom {}))

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
  (swap! queries #(assoc % query-hash results)))

(defn reverse-cache-add
  [id code]
  (if (contains? @reverse-cache id)
    (swap! reverse-cache #(update-in % [id] (fn [v] (conj v code))))
    (swap! reverse-cache #(assoc % id (list code)))))

(defn reverse-cache-get
  [id]
  (get @reverse-cache id))

(defn reverse-cache-delete
  [ids]
  (swap! reverse-cache #(apply dissoc (cons % ids))))

(defn clear-model-cache
  [ids]
  (swap! queries #(apply (partial dissoc %)
                         (mapcat (fn [id] (reverse-cache-get id)) ids)))
  (reverse-cache-delete ids))

(defn clear-queries
  []
  (swap! queries (fn [_] {}))
  (swap! reverse-cache (fn [_] {})))


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
