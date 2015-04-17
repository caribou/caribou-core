(ns caribou.db.adapter.h2
  (:require [clojure.java.jdbc.deprecated :as old-sql]
            [clojure.java.jdbc :as sql]
            [clojure.string :as string]
            [caribou.util :as util]
            [caribou.logger :as log])
  (:use [caribou.db.adapter.protocol :only (DatabaseAdapter)]))

(import org.h2.tools.Server)

(defn- pull-metadata
  [res]
  (doall (map #(string/lower-case (.getString res (int %)))
              (take 4 (iterate inc 1)))))

(defn- public-table?
  [[database role table kind]]
  (and (= "public" role) (= "table" kind)))

(defn h2-tables
  "Retrieve a list of all tables in an h2 database."
  []
  (let [connection (old-sql/connection)
        res (-> connection .getMetaData
                (.getTables (-> connection .getCatalog) nil nil nil))]
    (loop [acc []]
      (let [status (.next res)]
        (if status
          (recur (cons (pull-metadata res) acc))
          (map #(nth % 2) (filter public-table? acc)))))))

(defn h2-table?
  "Determine if the given table exists in the database."
  [table]
  (let [tables (h2-tables)
        table-name (util/dbize table)]
    (some #(= % table-name) tables)))

(defn h2-set-required
  [table column value]
  (old-sql/do-commands
   (log/out :db (util/clause
                   (if value
                     "alter table %1 alter column %2 set not null"
                     "alter table %1 alter column %2 drop not null")
                   [(util/dbize table) (util/dbize column)]))))

(defn h2-rename-column
  [table column new-name]
  (try
    (let [alter-statement "alter table %1 alter column %2 rename to %3"
          rename (log/out
                  :db
                  (util/clause alter-statement (map util/dbize [table column new-name])))]
      (old-sql/do-commands rename))
    (catch Exception e (log/render-exception e))))

(defn h2-drop-index
  [table column]
  (try
    (old-sql/do-commands
     (log/out :db (util/clause "drop index %1_%2_index"
                            (map util/dbize [table column]))))
    (catch Exception e (log/render-exception e))))

(defn h2-text-value
  [text]
  (if text
    (let [len (.length text)]
      (.getSubString text 1 len))
    ""))

(defn h2-build-subname
  [{:keys [protocol path database] :as config}]
  (assoc config :subname (str protocol ":" path database)))

(defrecord H2Adapter [config]
  DatabaseAdapter
  (init [this])
  (table? [this table] (h2-table? table))
  (build-subname [this config]
    (h2-build-subname config))
  (unicode-supported? [this] true)
  (supports-constraints? [this] false)
  (insert-result [this table result]
    (old-sql/with-query-results res
      [(str "select * from " (util/dbize table)
            " where id = " (result (first (keys result))))]
      (first (doall res))))
  (rename-column [this table column new-name]
    (h2-rename-column table column new-name))
  (set-required [this table column value]
    (h2-set-required table column value))
  (drop-index [this table column]
    (h2-drop-index table column))
  (drop-model-index [this old-table new-table column]
    (h2-drop-index old-table column))
  (text-value [this text]
    (h2-text-value text)))

