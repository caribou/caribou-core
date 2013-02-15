(ns caribou.db.adapter.mysql
  (:use caribou.debug
        caribou.util
        [caribou.db.adapter.protocol :only (DatabaseAdapter)])
  (:require [clojure.java.jdbc :as sql]))

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
     (log :db (clause
               (if value
                 "alter table %1 modify %2 %3 not null"
                 "alter table %1 modify %2 %3")
               [(zap table) (zap column) field-type])))))

(defn mysql-rename-column
  [table column new-name]
  (try
    (let [field-type (find-column-type table column)
          alter-statement "alter table %1 change %2 %3 %4"
          rename (log :db (clause alter-statement (map name [table column new-name field-type])))]
      (sql/do-commands rename))
    (catch Exception e (render-exception e))))

(defrecord MysqlAdapter [config]
  DatabaseAdapter
  (init [this])
  (unicode-supported? [this]
    (let [result (query "show variables like 'character_set_database'")]
      (println "DEBUG character_set_database" (str result))
      (= "utf8" (:value (first result)))))
  (table? [this table]
    (mysql-table? table))
  (build-subname [this config]
    (let [host (or (config :host) "localhost")
          subname (or (config :subname) (str "//" host "/" (config :database)
                                             "?useUnicode=true"
                                             "&characterEncoding=UTF-8"))]
      (assoc config :subname subname)))
  (insert-result [this table result]
    (sql/with-query-results res
      [(str "select * from " (name table)
            " where id = " (result (first (keys result))))]
      (first (doall res))))
  (rename-column [this table column new-name]
    (mysql-rename-column table column new-name))
  (set-required [this table column value]
    (mysql-set-required table column value))
  (text-value [this text]
    text))
