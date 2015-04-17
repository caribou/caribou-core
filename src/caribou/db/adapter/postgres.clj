(ns caribou.db.adapter.postgres
  (:use [caribou.db.adapter.protocol :only (DatabaseAdapter)])
  (:require [caribou.logger :as log]
            [caribou.util :as util]
            [clojure.java.jdbc.deprecated :as old-sql]))

(import java.util.regex.Matcher)

(defn postgres-table?
  "Determine if this table exists in the postgresql database."
  [table]
  (< 0
     (count
      (util/query "select true from pg_class where relname='%1'"
             (util/dbize (name table))))))

(defn postgres-set-required
  [table column value]
  (old-sql/do-commands
   (log/out :db (util/clause
                   (if value
                     "alter table %1 alter column %2 set not null"
                     "alter table %1 alter column %2 drop not null")
                   [(util/dbize table) (util/dbize column)]))))

(defn postgres-rename-column
  [table column new-name]
  (try
    (let [alter-statement "alter table %1 rename column %2 to %3"
          rename (log/out :db (util/clause alter-statement (map util/dbize [table column new-name])))]
      (old-sql/do-commands rename))
    (catch Exception e (log/render-exception e))))

(defn postgres-drop-index
  [table column]
  (try
    (old-sql/do-commands
     (log/out :db (util/clause "drop index %1_%2_index" (map util/dbize [table column]))))
    (catch Exception e (log/render-exception e))))

(defrecord PostgresAdapter [config]
  DatabaseAdapter
  (init [this])
  (table? [this table]
    (postgres-table? table))
  (unicode-supported? [this] true)
  (supports-constraints? [this] true)
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
