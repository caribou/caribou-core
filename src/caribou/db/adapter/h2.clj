(ns caribou.db.adapter.h2
  (:require [clojure.java.jdbc :as sql]
            [clojure.string :as string])
  (:use [caribou.db.adapter.protocol :only (DatabaseAdapter)]))

(import org.h2.tools.Server)

(defn pull-metadata
  [res]
  (doall (map #(string/lower-case (.getString res (int %))) (take 4 (iterate inc 1)))))

(defn public-table?
  [[database role table kind]]
  (and (= "public" role) (= "table" kind)))

(defn h2-tables
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
  [table]
  (let [tables (h2-tables)
        table-name (name table)]
    (some #(= % table-name) tables)))

(def h2-server (ref nil))

(defrecord H2Adapter [config]
  DatabaseAdapter
  (init [this]
    (let [connection (str "jdbc:h2:tcp://" (config :host) "/" (config :database))
          server (Server/createTcpServer (into-array [connection "-tcpAllowOthers" "true"]))]
      (println "init: " connection)
      (.start server)
      (dosync (ref-set h2-server server))))
  (table? [this table]
    (h2-table? table))
  (build-subname [this config]
    (let [host (or (config :host) "localhost")
          pathos (or (config :db-path) "/../")
          config (dissoc config :db-path)
          connection (str "tcp://" host pathos (config :database))
          subname (or (config :subname) connection)] ;; ";AUTO_SERVER=TRUE"))]
      (println "build subname: " connection)
      (assoc config :subname subname)))
  (insert-result [this table result]
    (sql/with-query-results res
      [(str "select * from " (name table)
            " where id = " (result (first (keys result))))]
      (first (doall res))))
  (text-value [this text]
    (string/replace (string/replace (str text) #"^'" "") #"'$" "")))
