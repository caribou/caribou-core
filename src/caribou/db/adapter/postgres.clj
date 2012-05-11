(ns caribou.db.adapter.postgres
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

(defn postgres-table?
  [table]
  (< 0
     (count
      (query "select true from pg_class where relname='%1'"
             (zap (name table))))))

(defrecord PostgresAdapter [config]
  DatabaseAdapter
  (table? [this table]
    (postgres-table? table))
  (insert-result [this table result]
    result)
  (text-value [this text]
    text))
