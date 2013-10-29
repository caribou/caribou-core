(ns caribou.gather
  (:require [caribou.util :as util]))

(defn query
  "Create a new map representing a gather"
  ([] {})
  ([model]
   {:model model}))

(defn- listify [v]
  "If a value isn't some sequential type, make it a vector"
  (if (sequential? v) v [v]))

(defn where
  "Add a where clause"
  ([q k v]
   (let [existing (or (:where q) {})
         new (assoc-in existing (listify k) v)]
     (assoc q :where new)))
  ([q clause]
    (let [existing (or (:where q) {})
          new (util/deep-merge-with merge existing clause)]
      (assoc q :where new))))

(defn limit
  "Add a limit term"
  [q limit]
  (assoc q :limit limit))

(defn offset
  "Add an offset term"
  [q offset]
  (assoc q :offset offset))

(defn include
  "Add an entry in the include map"
  [q entry]
  (let [existing (or (:include q) {})
        new (util/deep-merge-with merge existing entry)]
    (assoc q :include new)))

(defn order-by
  "Add an order entry in the order map"
  [q entry]
  (let [existing (or (:order q) {})
        new (util/deep-merge-with merge existing entry)]
    (assoc q :order new)))

(def order order-by)

;;; Examples of how to use these:

(comment
  (-> (query)
      (where {:a "foo"})))

(comment
  (-> (query)
      (where {:a "this"})
      (where {:b "that"})
      (where {:c {:d "another"}})))

(comment
  (-> (query)
      (where {:a "foo"})
      (limit 10)))

(comment
  (-> (query)
      (where {:a "foo"})
      (offset 10)))

(comment
  (-> (query)
      (where {:a "foo"})
      (include {:this {}})
      (include {:that {:another {}}})))

(comment
  (-> (query :thing)
      (where {:a "foo"})
      (order-by {:this :asc})
      (include {:that {:another {}}})))

(comment
  (-> (query)
      (where {:a "foo"})
      (include {:thing {}})
      (order {:foo :asc})))


(comment
  (-> (query)
      (where :a "foo")
      (where [:b :c :e] "bar")))
