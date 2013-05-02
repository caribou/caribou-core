 (ns caribou.db.adapter.mysql
  (:use caribou.util
        [caribou.db.adapter.protocol :only (DatabaseAdapter)])
  (:require [caribou.logger :as log]
            [clojure.java.jdbc :as sql]))

(import java.util.regex.Matcher)

(defn mysql-table?
  "Determine if this table exists in the mysql database."
  [table]
  (if (query "show tables like '%1'" (zap (name table)))
    true false))

(defn find-column-type
  [table column]
  (let [result (query "show fields from %1 where Field = '%2'" (zap table) (zap column))]
    (-> result first :type)))

(defn mysql-set-required
  [table column value]
  (let [field-type (find-column-type table column)]
    (sql/do-commands
     (log/out :db (clause
                     (if value
                       "alter table %1 modify %2 %3 not null"
                       "alter table %1 modify %2 %3")
                     [(zap table) (zap column) field-type])))))

(defn mysql-rename-column
  [table column new-name]
  (try
    (let [field-type (find-column-type table column)
          alter-statement "alter table %1 change %2 %3 %4"
          rename (log/out :db (clause alter-statement (map (comp zap name) [table column new-name field-type])))]
      (sql/do-commands rename))
    (catch Exception e (log/render-exception e))))

(defn mysql-insert-result
  [this table result]
  (sql/with-query-results res
    [(str "select * from " (name table)
          " where id = " (result (first (keys result))))]
    (first (doall res))))

(defn mysql-drop-index
  [table column]
  (try
    (sql/do-commands
     (log/out :db (clause "alter table %1 drop index %1_%2_index" (map (comp zap name) [table column]))))
    (catch Exception e (log/render-exception e))))

(defn mysql-drop-model-index
  [old-table new-table column]
  (try
    (sql/do-commands
     (log/out :db (clause "alter table %2 drop index %1_%3_index" (map (comp zap name) [old-table new-table column]))))
    (catch Exception e (log/render-exception e))))

(defrecord MysqlAdapter [config]
  DatabaseAdapter
  (init [this])
  (unicode-supported? [this]
    (let [result (query "show variables like 'character_set_database'")]
      (log/debug (str "character_set_database" result))
      (= "utf8" (:value (first result)))))
  (supports-constraints? [this] true)
  (table? [this table]
    (mysql-table? table))
  (build-subname [this config]
    (let [host (or (config :host) "localhost")
          subname (or (config :subname) (str "//" host "/" (config :database)
                                             "?useUnicode=true"
                                             "&characterEncoding=UTF-8"
                                             "&zeroDateTimeBehavior=convertToNull"))]
      (assoc config :subname subname)))
  (insert-result [this table result]
    (mysql-insert-result this table result))
  (rename-column [this table column new-name]
    (mysql-rename-column table column new-name))
  (set-required [this table column value]
    (mysql-set-required table column value))
  (drop-index [this table column]
    (mysql-drop-index table column))
  (drop-model-index [this old-table new-table column]
    (mysql-drop-model-index old-table new-table column))
  (text-value [this text]
    text))
