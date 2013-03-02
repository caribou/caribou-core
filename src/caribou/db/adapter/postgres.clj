(ns caribou.db.adapter.postgres
  (:use caribou.util
        [caribou.db.adapter.protocol :only (DatabaseAdapter)])
  (:require [caribou.debug :as debug]
            [caribou.logger :as log]
            [clojure.java.jdbc :as sql]))

(import java.util.regex.Matcher)

(defn postgres-table?
  "Determine if this table exists in the postgresql database."
  [table]
  (< 0
     (count
      (query "select true from pg_class where relname='%1'"
             (zap (name table))))))

(defn postgres-set-required
  [table column value]
  (sql/do-commands
   (debug/out :db (clause
                   (if value
                     "alter table %1 alter column %2 set not null"
                     "alter table %1 alter column %2 drop not null")
                   [(zap table) (zap column)]))))

(defn postgres-rename-column
  [table column new-name]
  (try
    (let [alter-statement "alter table %1 rename column %2 to %3"
          rename (debug/out :db (clause alter-statement (map (comp zap name) [table column new-name])))]
      (sql/do-commands rename))
    (catch Exception e (render-exception e))))

(defn postgres-drop-index
  [table column]
  (try
    (sql/do-commands
     (debug/out :db (clause "drop index %1_%2_index" (map (comp zap name) [table column]))))
    (catch Exception e (render-exception e))))

(defrecord PostgresAdapter [config]
  DatabaseAdapter
  (init [this])
  (table? [this table]
    (postgres-table? table))
  (unicode-supported? [this] true)
  (build-subname [this config]
    (let [host (or (config :host) "localhost")
          subname (or (config :subname) (str "//" host "/" (config :database)))]
      (assoc config :subname subname)))
  (insert-result [this table result]
    result)
  (rename-column [this table column new-name]
    (postgres-rename-column table column new-name))
  (set-required [this table column value]
    (postgres-set-required table column value))
  (drop-index [this table column]
    (postgres-drop-index table column))
  (drop-model-index [this old-table new-table column]
    (postgres-drop-index old-table column))
  (text-value [this text]
    text))
