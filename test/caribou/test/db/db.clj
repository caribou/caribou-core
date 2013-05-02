(ns caribou.test.db.db
  (:use [clojure.test]
        [caribou.util]
        [caribou.test]
        [caribou.db.adapter.protocol :as adapter])
  (:require [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [caribou.config :as config]
            [caribou.db :as db]
            [caribou.core :as core]))

(defn db-fixture
  [f config]
  (core/with-caribou config
    (f)
    (doseq [tmp-table ["veggies" "fruit"]]
      (when (db/table? tmp-table)
        (db/drop-table tmp-table)))))

(defn unicode-support-test
  []
  (is (adapter/unicode-supported? (config/draw :db :adapter))))

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

(defn all-db-tests
  [config]
  (db-fixture unicode-support-test config)
  (db-fixture query-test config)
  (db-fixture query2-exception-test config)
  (db-fixture choose-test config)
  (db-fixture table-test config)
  (db-fixture create-new-table-drop-table-test config)
  (db-fixture add-column-test config))

(deftest ^:mysql
  mysql-tests
  (let [config (read-config :mysql)]
    (all-db-tests config)))

(deftest ^:postgres
  postgres-tests
  (let [config (read-config :postgres)]
    (all-db-tests config)))

(deftest ^:h2
  h2-tests
  (let [config (read-config :h2)]
    (all-db-tests config)))
