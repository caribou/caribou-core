(ns caribou.test.db.db
  (:use [clojure.test]
        [caribou.util]
        [caribou.db.adapter.protocol :as adapter])
  (:require [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]
            [caribou.config :as config]
            [clojure.string :as string]
            [caribou.db :as db]))

(defn db-fixture
  [f]
  (sql/with-connection @config/db
    (f)
    (doseq [tmp-table ["veggies" "fruit"]]
      (when (db/table? tmp-table)
        (db/drop-table tmp-table)))))

(defn unicode-support-test
  []
  (is (adapter/unicode-supported? @config/db-adapter)))

;; TODO: test database...
;; query
(defn query-test
  []
  (let [q (query "select * from model")]
    (is (not (empty? q)))))

(defn query2-exception-test
  []
  (let [unknown-regexes ["relation .* does not exist"
                         "Table .* doesn't exist"
                         "Table .* not found"]
        unknown-re (string/join "|" unknown-regexes)]
    (is (thrown-with-msg? Exception (re-pattern unknown-re)
          (query "select * from modelz")))))

;; TODO: insert / update / delete / fetch

(defn choose-test
  []
  (is (= (get (db/choose :model 1) :name) "Model"))
  (is (= (get (db/choose :model 0) :name) nil)))

(defn table-test
  []
  (is (db/table? "model"))
  (is (not (db/table? "modelzzzz"))))

(defn create-new-table-drop-table-test
  []
  (let [tmp-table "veggies"]
    (db/create-table tmp-table [:id "SERIAL" "PRIMARY KEY"])
    (is (db/table? tmp-table))))

;; TODO: test to ensure options applied to column
(defn add-column-test
  []
  (let [tmp-table "fruit"]
    (db/create-table tmp-table [:id "SERIAL" "PRIMARY KEY"])
    (db/add-column tmp-table "jopotonio" [:integer])))

(deftest ^:mysql
  mysql-tests
  (let [config (config/read-config (io/resource "config/test-mysql.clj"))]
    (config/configure config)
    (db-fixture unicode-support-test)
    (db-fixture query-test)
    (db-fixture query2-exception-test)
    (db-fixture choose-test)
    (db-fixture table-test)
    (db-fixture create-new-table-drop-table-test)
    (db-fixture add-column-test)))

(deftest ^:postgres
  postgres-tests
  (let [config (config/read-config (io/resource "config/test-postgres.clj"))]
    (config/configure config)
    (db-fixture unicode-support-test)
    (db-fixture query-test)
    (db-fixture query2-exception-test)
    (db-fixture choose-test)
    (db-fixture table-test)
    (db-fixture create-new-table-drop-table-test)
    (db-fixture add-column-test)))

(deftest ^:h2
  h2-tests
  (let [config (config/read-config (io/resource "config/test-h2.clj"))]
    (config/configure config)
    (db-fixture unicode-support-test)
    (db-fixture query-test)
    (db-fixture query2-exception-test)
    (db-fixture choose-test)
    (db-fixture table-test)
    (db-fixture create-new-table-drop-table-test)
    (db-fixture add-column-test)))
