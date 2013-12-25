(ns caribou.db
  (:use [clojure.string :only (join split)])
  (:require [clojure.string :as string]
            [clojure.java.jdbc :as sql]
            [caribou.logger :as log]
            [caribou.config :as config]
            [caribou.util :as util]
            [caribou.db.adapter.protocol :as adapter]))

(import java.util.regex.Matcher)

(defn query
  "make an arbitrary query, substituting in extra args as % parameters"
  ([template] (query template []))
  ([template args]
     (let [q (vec (cons template args))]
       (sql/with-query-results res
         q
         (doall res)))))

(defn recursive-query [table fields base-where recur-where]
  (let [field-names (distinct (map name (concat [:id :parent-id] fields)))
        field-list (join "," field-names)]
    (util/query (str "with recursive %1_tree(" field-list
                ") as (select " field-list
                " from %1 where %2 union select "
                (join "," (map #(str "%1." %) field-names))
                " from %1,%1_tree where %3)"
                " select * from %1_tree") (util/dbize table) base-where recur-where)))

(defn sqlize
  "process a raw value into a sql appropriate string"
  [value]
  (cond
    (number? value) value
    (isa? (type value) Boolean) value
    (symbol? value) (str value)
    (keyword? value) (util/dbize value)
    (string? value) (str "'" (util/zap value) "'")
    :else (str "'" (util/zap (str value)) "'")))

(defn value-map
  "build a string of values fit for an insert or update statement"
  [values]
  (join ", " (map #(str (name %) " = " (sqlize (values %))) (keys values))))

(defn insert
  "insert a row into the given table with the given values"
  [table values]
  (log/out :db (util/clause "insert into %1 values %2" [(util/dbize table) (value-map values)]))
  (let [result (sql/insert-record (util/dbize table) values)]
    (adapter/insert-result (config/draw :database :adapter) (util/dbize table) result)))

(defn update
  "update the given row with the given values"
  [table where values]
  (log/out :db (str "update " table " " where " set " values))
  (if (not (empty? values))
    (try
      (sql/update-values (util/dbize table) where values)
      (catch Exception e
        (log/render-exception e)))))

(defn delete
  "delete out of the given table according to the supplied where clause"
  [table & where]
  (log/out :db (util/clause "delete from %1 values %2" [(util/dbize table) (util/clause (first where) (rest where))]))
  (sql/delete-rows
   (util/dbize table)
   where))

(defn fetch
  "pull all items from a table according to the given conditions"
  [table & where]
  (query (str "select * from " (util/dbize table)
              " where " (first where))
         (rest where)))

(defn choose
  "pull just the record with the given id from the given table"
  [table id]
  (if id
    (first (query (str "select * from " (util/dbize table) " where id = ?") [(util/convert-int id)]))))

(defn tally
  "return how many total records are in this table"
  [table]
  (let [result (first (query (str "select count(id) from " (util/dbize table))))]
    (result (first (keys result)))))

(defn find-model
  [id models]
  (or (get models id) (choose :model id)))

(defn commit
  []
  (sql/do-commands "commit"))

;; table operations -------------------------------------------

(defn table?
  "check to see if a table by the given name exists"
  [table]
  (adapter/table? (config/draw :database :adapter) table))

(defn create-table
  "create a table with the given columns, of the format
  [:column-name :type & :extra]"
  [table & fields]
  (log/out :db (util/clause "create table %1 %2" [(util/dbize table) fields]))
  (try
    (apply sql/create-table (cons table fields))
    (catch Exception e (log/render-exception e))))

(defn rename-table
  "change the name of a table to new-name."
  [table new-name]
  (let [rename (log/out :db (util/clause "alter table %1 rename to %2" [(util/dbize table) (util/dbize new-name)]))]
    (try
      (sql/do-commands rename)
      (catch Exception e (log/render-exception e)))))

(defn drop-table
  "remove the given table from the database."
  [table]
  (let [drop (log/out :db (util/clause "drop table %1 cascade" [(util/dbize table)]))]
    (try
      (sql/do-commands drop)
      (catch Exception e (log/render-exception e)))))

(defn add-column
  "add the given column to the table."
  [table column opts]
  (let [type (join " " (map util/dbize opts))]
    (try
      (sql/do-commands
       (log/out :db (util/clause "alter table %1 add column %2 %3" (map util/dbize [table column type]))))
      (catch Exception e (log/render-exception e)))))

(defn rename-column
  "rename a column in the given table to new-name."
  [table column new-name]
  (adapter/rename-column (config/draw :database :adapter) table column new-name))

(defn drop-column
  "remove the given column from the table."
  [table column]
  (sql/do-commands
   (log/out :db (util/clause "alter table %1 drop column %2" (map util/dbize [table column])))))

(defn create-index
  [table column]
  (if (adapter/supports-constraints? (config/draw :database :adapter))
    (try
      (sql/do-commands
       (log/out :db (util/clause "create index %1_%2_index on %1 (%2)" (map util/dbize [table column]))))
      (catch Exception e (log/render-exception e)))))

(defn drop-index
  [table column]
  (if (adapter/supports-constraints? (config/draw :database :adapter))
    (adapter/drop-index (config/draw :database :adapter) (util/dbize table) (util/dbize column))))

(defn drop-model-index
  [old-table new-table column]
  (if (adapter/supports-constraints? (config/draw :database :adapter))
    (adapter/drop-model-index (config/draw :database :adapter) (util/dbize old-table) (util/dbize new-table) (util/dbize column))))

(defn set-default
  "sets the default for a column"
  [table column default]
  (try
    (if (adapter/supports-constraints? (config/draw :database :adapter))
      (let [value (sqlize default)]
        (sql/do-commands
         (log/out :db (util/clause "alter table %1 alter column %2 set default %3" [(util/dbize table) (util/dbize column) value])))))
    (catch Exception e (log/render-exception e))))

(defn set-required
  [table column value]
  (if (adapter/supports-constraints? (config/draw :database :adapter))
    (adapter/set-required (config/draw :database :adapter) (util/dbize table) (util/dbize column) value)))

(defn set-unique
  [table column value]
  (if (adapter/supports-constraints? (config/draw :database :adapter))
    (sql/do-commands
     (log/out :db (util/clause
                     (if value
                       "alter table %1 add constraint %1_%2_unique unique (%2)"
                       "alter table %1 drop constraint %1_%2_unique")
                     [(util/dbize table) (util/dbize column)])))))

(defn add-primary-key
  [table column]
  (if (adapter/supports-constraints? (config/draw :database :adapter))
    (try
      (sql/do-commands
       (log/out
        :db
        (util/clause "alter table %1 add primary key (%2)" [(util/dbize table) (util/dbize column)])))
      (catch Exception e (log/render-exception e)))))

(defn add-reference
  [table column reference deletion]
  (if (adapter/supports-constraints? (config/draw :database :adapter))
    (try
      (sql/do-commands
       (log/out :db (util/clause
                       (condp = deletion
                         :destroy "alter table %1 add constraint %1_%2_%3_fk foreign key(%2) references %3 on delete cascade"
                         :default "alter table %1 add constraint %1_%2_%3_fk foreign key(%2) references %3 on delete set default"
                         "alter table %1 add constraint %1_%2_%3_fk foreign key(%2) references %3 on delete set null")
                       [(util/dbize table) (util/dbize column) (util/dbize reference)])))
      (catch Exception e
        (log/error (str "UNABLE TO ADD REFERENCE FOR" table column reference deletion e))))))

(defn drop-reference
  [table column reference]
  (if (adapter/supports-constraints? (config/draw :database :adapter))
    (try
      (sql/do-commands
       (log/out 
        :db 
        (util/clause
         "alter table %1 drop constraint %1_%2_%3_fk"
         [(util/dbize table) (util/dbize column) (util/dbize reference)])))
      (catch Exception e
        (log/error (str "UNABLE TO DROP REFERENCE FOR" table column reference e))))))

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

(defn drop-database
  "drop a database of the given config"
  [config]
  (let [db-name (config :database)]
    (log/debug (str "dropping database: " db-name))
    (try
      (sql/with-connection (change-db-keep-host config "template1")
        (with-open [s (.createStatement (sql/connection))]
          (.addBatch s (str "drop database " (util/dbize db-name)))
          (seq (.executeBatch s))))
      (catch Exception e (log/render-exception e)))))

(defn create-database
  "create a database of the given name"
  [config]
  (let [db-name (config :database)]
    (log/debug (str "creating database: " db-name))
    (try
      (sql/with-connection (change-db-keep-host config "template1") 
        (with-open [s (.createStatement (sql/connection))]
          (.addBatch s (str "create database " (util/dbize db-name)))
          (seq (.executeBatch s))))
      (catch Exception e (log/render-exception e)))))

(defn rebuild-database
  "drop and recreate the given database"
  [config]
  (drop-database config)
  (create-database config))

(defn call
  [f]
  (sql/with-connection (config/draw :database) (f)))

(defn wrap-db
  [handler db & [opts]]
  (if (config/draw :app :use-database)
    (fn [request]
      (sql/with-connection db (handler request)))
    (fn [request]
      (handler request))))

(defmacro with-db
  [config & body]
  `(config/with-config ~config
     (if (config/draw :app :use-database)
       (sql/with-naming-strategy caribou.util/naming-strategy
         (sql/with-connection (config/draw :database)
           ~@body))
       (do ~@body))))
