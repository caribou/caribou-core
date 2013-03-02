(ns caribou.db.adapter.h2
  (:use caribou.util)
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as string]
            [caribou.logger :as log]
            [caribou.debug :as debug])
  (:use [caribou.db.adapter.protocol :only (DatabaseAdapter)]))

(import org.h2.tools.Server)

(defn- pull-metadata
  [res]
  (doall (map #(string/lower-case (.getString res (int %))) (take 4 (iterate inc 1)))))

(defn- public-table?
  [[database role table kind]]
  (and (= "public" role) (= "table" kind)))

(defn h2-tables
  "Retrieve a list of all tables in an h2 database."
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
  "Determine if the given table exists in the database."
  [table]
  (let [tables (h2-tables)
        table-name (name table)]
    (some #(= % table-name) tables)))

(defn h2-set-required
  [table column value]
  (sql/do-commands
   (debug/out :db (clause
                   (if value
                     "alter table %1 alter column %2 set not null"
                     "alter table %1 alter column %2 drop not null")
                   [(zap table) (zap column)]))))

(def h2-server (ref nil))

(defn h2-rename-column
  [table column new-name]
  (try
    (let [alter-statement "alter table %1 alter column %2 rename to %3"
          rename (debug/out :db (clause alter-statement (map name [table column new-name])))]
      (sql/do-commands rename))
    (catch Exception e (render-exception e))))

(defn h2-init
  [adapter config]
  ;; (let [connection (str "jdbc:h2:tcp://" (config :host) "/" (config :database))
  ;;       server (Server/createTcpServer
  ;;               (into-array [connection "-tcp" "-tcpAllowOthers"]))]
  ;;   (log/info (str "init: " connection))
  ;;   (.start server)
  ;;   (dosync (ref-set h2-server server))))
  (let [connection (str "jdbc:h2:tcp://" (config :host) "/" (config :database))
        server (Server/createTcpServer
                (into-array [connection "-tcpAllowOthers" "true"]))]
    (log/info (str "init: " connection))
    (.start server)
    (dosync (ref-set h2-server server))))

(defn h2-drop-index
  [table column]
  (try
    (sql/do-commands
     (debug/out :db (clause "drop index %1_%2_index" (map #(zap (name %)) [table column]))))
    (catch Exception e (render-exception e))))

(defrecord H2Adapter [config]
  DatabaseAdapter

  (init [this]
    (h2-init this config))

  (table? [this table]
    (h2-table? table))

  (build-subname [this config]
    (let [host (or (config :host) "localhost")
          pathos (or (config :db-path) "/../")
          config (dissoc config :db-path)
          connection (str "tcp://" host pathos (config :database))
          subname (or (config :subname) connection)] ;; ";AUTO_SERVER=TRUE"))]
      (log/info (str "build subname: " connection))
      (assoc config :subname subname)))

  (insert-result [this table result]
    (sql/with-query-results res
      [(str "select * from " (name table)
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
    (string/replace (string/replace (str text) #"^'" "") #"'$" "")))
