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

(defrecord MysqlAdapter [config]
  DatabaseAdapter
  (init [this])
  (table? [this table]
    (mysql-table? table))
  (build-subname [this config]
    (let [host (or (config :host) "localhost")
          subname (or (config :subname) (str "//" host "/" (config :database)))]
      (assoc config :subname subname)))
  (insert-result [this table result]
    (sql/with-query-results res
      [(str "select * from " (name table)
            " where id = " (result (first (keys result))))]
      (first (doall res))))
  (rename-clause [this]
    "alter table %1 rename column %2 to %3")
  (set-required [this table column value]
    (mysql-set-required table column value))
  (text-value [this text]
    text))
