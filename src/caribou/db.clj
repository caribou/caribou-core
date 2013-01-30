(ns caribou.db
  (:use caribou.debug
        caribou.util
        [clojure.string :only (join split)])
  (:require [clojure.string :as string]
            [clojure.java.jdbc :as sql]
            [caribou.config :as config]
            [caribou.db.adapter.protocol :as adapter]))

(import java.util.regex.Matcher)

(defn recursive-query [table fields base-where recur-where]
  (let [field-names (distinct (map name (concat [:id :parent_id] fields)))
        field-list (join "," field-names)]
    (query (str "with recursive %1_tree(" field-list
                ") as (select " field-list
                " from %1 where %2 union select "
                (join "," (map #(str "%1." %) field-names))
                " from %1,%1_tree where %3)"
                " select * from %1_tree") (name table) base-where recur-where)))

(defn sqlize
  "process a raw value into a sql appropriate string"
  [value]
  (cond
   (number? value) value
   (isa? (type value) Boolean) value
   (keyword? value) (zap (name value))
   (string? value) (str "'" (zap value) "'")
   :else (str "'" (zap (str value)) "'")))

(defn value-map
  "build a string of values fit for an insert or update statement"
  [values]
  (join ", " (map #(str (name %) " = " (sqlize (values %))) (keys values))))

(defn insert
  "insert a row into the given table with the given values"
  [table values]
  ;; (let [keys (join "," (map sqlize (keys mapping)))
  ;;       values (join "," (map sqlize (vals mapping)))
  ;;       q (clause "insert into %1 (%2) values (%3)" [(zap (name table)) keys values])]
  ;;   (sql/with-connection db
  ;;     (sql/do-commands
  ;;       (log :db q)))))
  (log :db (clause "insert into %1 values %2" [(name table) (value-map values)]))
  (let [result (sql/insert-record table values)]
    (adapter/insert-result @config/db-adapter (name table) result)))

(defn update
  "update the given row with the given values"
  [table where values]
  (log :db (str "update " table " " where " set " values))
  (if (not (empty? values))
    (try
      (sql/update-values table where values)
      (catch Exception e
        (try
          (println (str "update " table " failed: " (.getNextException (debug e))))
          (catch Exception e (println e)))))))

;; (defn update
;;   "update the given row with the given values"
;;   [table values & where]
;;   (let [v (value-map values)
;;         q (clause "update %1 set %2 where " [(zap (name table)) v])
;;         w (clause (first where) (rest where))
;;         t (str q w)]
;;     (sql/do-commands (log :db t))))

(defn delete
  "delete out of the given table according to the supplied where clause"
  [table & where]
  (log :db (clause "delete from %1 values %2" [(name table) (clause (first where) (rest where))]))
  (sql/delete-rows (name table) [(if (not (empty? where)) (clause (first where) (rest where)))]))

(defn fetch
  "pull all items from a table according to the given conditions"
  [table & where]
  (apply query (cons (str "select * from %" (count where) " where " (first where))
                     (concat (rest where) [(name table)]))))

(defn choose
  "pull just the record with the given id from the given table"
  [table id]
  (if id
    (first (query "select * from %1 where id = %2" (zap (name table)) (zap (str id))))
    nil))

(defn commit
  []
  (sql/do-commands "commit"))

;; table operations -------------------------------------------

(defn table?
  "check to see if a table by the given name exists"
  [table]
  (adapter/table? @config/db-adapter table))

(defn create-table
  "create a table with the given columns, of the format
  [:column_name :type & :extra]"
  [table & fields]
  (log :db (clause "create table %1 %2" [(name table) fields]))
  (try
    (apply sql/create-table (cons table fields))
    (catch Exception e (render-exception e))))

(defn rename-table
  "change the name of a table to new-name."
  [table new-name]
  (let [rename (log :db (clause "alter table %1 rename to %2" [(name table) (name new-name)]))]
    (try
      (sql/do-commands rename)
      (catch Exception e (render-exception e)))))

(defn drop-table
  "remove the given table from the database."
  [table]
  (let [drop (log :db (clause "drop table %1 cascade" [(name table)]))]
    (try
      (sql/do-commands drop)
      (catch Exception e (render-exception e)))))

(defn add-column
  "add the given column to the table."
  [table column opts]
  (let [type (join " " (map name opts))]
    (try
      (sql/do-commands
       (log :db (clause "alter table %1 add column %2 %3" (map #(zap (name %)) [table column type]))))
      (catch Exception e (render-exception e)))))

(defn create-index
  [table column]
  (try
    (sql/do-commands
     (log :db (clause "create index %1_%2_index on %1 (%2)" (map #(zap (name %)) [table column]))))
    (catch Exception e (render-exception e))))

(defn set-default
  "sets the default for a column"
  [table column default]
  (let [value (sqlize default)]
    (sql/do-commands
     (log :db (clause "alter table %1 alter column %2 set default %3" [(zap table) (zap column) value])))))

(defn set-required
  [table column value]
  (adapter/set-required @config/db-adapter (name table) (name column) value))
  ;; (sql/do-commands
  ;;  (log :db (clause
  ;;            (if value
  ;;              "alter table %1 alter column %2 set not null"
  ;;              "alter table %1 alter column %2 drop not null")
  ;;            [(zap table) (zap column)]))))

(defn set-unique
  [table column value]
  (sql/do-commands
   (log :db (clause
             (if value
               "alter table %1 add constraint %2_unique unique (%2)"
               "alter table %1 drop constraint %2_unique")
             [(zap table) (zap column)]))))

(defn add-primary-key
  [table column]
  (try
    (sql/do-commands
     (log
      :db
      (clause "alter table %1 add primary key (%2)" [(zap table) (zap column)])))
    (catch Exception e (render-exception e))))

(defn add-reference
  [table column reference deletion]
  (try
    (sql/do-commands
     (log :db (clause
               (condp = deletion
                 :destroy "alter table %1 add foreign key(%2) references %3 on delete cascade"
                 :default "alter table %1 add foreign key(%2) references %3 on delete set default"
                 "alter table %1 add foreign key(%2) references %3 on delete set null")
               [(zap table) (zap column) (zap reference)])))
    (catch Exception e (println "UNABLE TO ADD REFERENCE FOR" table column reference deletion))))
;; (render-exception e))))

(defn rename-column
  "rename a column in the given table to new-name."
  [table column new-name]
  (adapter/rename-column @config/db-adapter table column new-name))

(defn drop-column
  "remove the given column from the table."
  [table column]
  (sql/do-commands
   (log :db (clause "alter table %1 drop column %2" (map #(zap (name %)) [table column])))))

(defn do-sql
  "execute arbitrary sql.  direct proxy to sql/do-commands."
  [commands]
  (try
    (sql/do-commands commands)
    (catch Exception e (.getNextException e))))

(defn change-db-keep-host
  "given the current db config, change the database but keep the hostname"
  [db-config new-db]
  (assoc db-config
    :subname (string/replace (db-config :subname) #"[^/]+$" new-db)))
;; (str "//" (first (split (replace (db-config :subname) "//" "") #"/")) "/" new-db)))

(defn drop-database
  "drop a database of the given config"
  [config]
  (let [db-name (config :database)]
    (println "dropping database: " db-name)
    (try
      (sql/with-connection (change-db-keep-host config "template1")
        (with-open [s (.createStatement (sql/connection))]
          (.addBatch s (str "drop database " (zap db-name)))
          (seq (.executeBatch s))))
      (catch Exception e (render-exception e)))))

(defn create-database
  "create a database of the given name"
  [config]
  (let [db-name (config :database)]
    (println "creating database: " db-name)
    (try
      (sql/with-connection (change-db-keep-host config "template1") 
        (with-open [s (.createStatement (sql/connection))]
          (.addBatch s (str "create database " (zap db-name)))
          (seq (.executeBatch s))))
      (catch Exception e (render-exception e)))))

(defn rebuild-database
  "drop and recreate the given database"
  [config]
  (drop-database config)
  (create-database config))

(defn call
  [f]
  (sql/with-connection @config/db (f)))

(defn wrap-db
  [handler db & [opts]]
  (if (:use-database @config/app)
    (fn [request]
      (sql/with-connection db (handler request)))
    (fn [request]
      (handler request))))

(defn tally
  "return how many total records are in this table"
  [table]
  (let [result (first (query "select count(id) from %1" (name table)))]
    (result (first (keys result)))))


