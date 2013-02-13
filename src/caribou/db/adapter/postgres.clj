(ns caribou.db.adapter.postgres
  (:use caribou.debug
        caribou.util
        [caribou.db.adapter.protocol :only (DatabaseAdapter)])
  (:require [clojure.java.jdbc :as sql]))

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
   (log :db (clause
             (if value
               "alter table %1 alter column %2 set not null"
               "alter table %1 alter column %2 drop not null")
             [(zap table) (zap column)]))))

(defn postgres-rename-column
  [table column new-name]
  (try
    (let [alter-statement "alter table %1 rename column %2 to %3"
          rename (log :db (clause alter-statement (map name [table column new-name])))]
      (sql/do-commands rename))
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
  (text-value [this text]
    text))
