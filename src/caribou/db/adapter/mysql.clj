(ns caribou.db.adapter.mysql
  (:use caribou.debug
        [caribou.db.adapter.protocol :only (DatabaseAdapter)])
  (:require [clojure.java.jdbc :as sql]))

(import java.util.regex.Matcher)

(defn- zap
  "quickly sanitize a potentially dirty string in preparation for a sql query"
  [s]
  (cond
   (string? s) (.replaceAll (re-matcher #"[\\\";#%]" (.replaceAll (str s) "'" "''")) "")
   (keyword? s) (zap (name s))
   :else s))

(defn- clause
  "substitute values into a string template based on numbered % parameters"
  [pred args]
  (letfn [(rep [s i] (.replaceAll s (str "%" (inc i))
                                  (let [item (nth args i)]
                                    (Matcher/quoteReplacement
                                     (cond
                                      (keyword? item) (name item)
                                      :else
                                      (str item))))))]
    (if (empty? args)
      pred
      (loop [i 0 retr pred]
        (if (= i (-> args count dec))
          (rep retr i)
          (recur (inc i) (rep retr i)))))))

(defn- query
  "make an arbitrary query, substituting in extra args as % parameters"
  [q & args]
  (sql/with-query-results res
    [(log :db (clause q args))]
    (doall res)))

(defn mysql-table?
  "Determine if this table exists in the mysql database."
  [table]
  (if (query "show tables like '%1'" (zap (name table)))
    true false))

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
  (text-value [this text]
    text))
