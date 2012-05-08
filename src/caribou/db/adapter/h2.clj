(ns caribou.db.adapter.h2
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as string])
  (:use [caribou.db.adapter.protocol :only (DatabaseAdapter)]))

(defn pull-metadata
  [res]
  (doall (map #(string/lower-case (.getString res (int %))) (take 4 (iterate inc 1)))))

(defn public-table?
  [[database role table kind]]
  (and (= "public" role) (= "table" kind)))

(defn h2-tables
  []
  (let [connection (sql/connection)
        res (-> connection .getMetaData
                (.getTables (-> connection .getCatalog) nil nil nil))]
    (loop [acc []]
      (let [status (.next res)]
        (if status
          (recur (cons (pull-metadata res) acc))
          (map #(nth % 2) (filter public-table? acc)))))))

(defn h2-table?
  [table]
  (let [tables (h2-tables)
        table-name (name table)]
    (some #(= % table-name) tables)))

(defrecord H2Adapter [config]
  DatabaseAdapter
  (table? [this table]
    (h2-table? table))
  (insert-result [this table result]
    (sql/with-query-results res
      [(str "select * from " table " where id = " (result (first (keys result))))]
      (first (doall res)))))
