(ns caribou.test.db.db
  (:use [clojure.test]
        [caribou.util])
  (:require [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]
            [caribou.config :as config]
            [caribou.db :as db]))

(def test-config (config/read-config (io/resource "config/test.clj")))

(config/configure test-config)

(defn db-fixture
  [f]
  (sql/with-connection @config/db
    (f)
    (doseq [tmp-table ["veggies" "fruit"]]
      (when (db/table? tmp-table)
        (db/drop-table tmp-table)))))

(use-fixtures :each db-fixture)

;; TODO: test database...
;; query
(deftest query-test
  (let [q (query "select * from model")]
    (is (not (empty? q)))))

(deftest query2-exception-test
  (is (thrown-with-msg? Exception #"relation .* does not exist"
        (query "select * from modelz"))))

;; TODO: insert / update / delete / fetch

(deftest choose-test
  (is (= (get (db/choose :model 1) :name) "Model"))
  (is (= (get (db/choose :model 0) :name) nil)))

(deftest table-test
  (is (db/table? "model"))
  (is (not (db/table? "modelzzzz"))))

(deftest create-new-table-drop-table-test
  (let [tmp-table "veggies"]
    (db/create-table tmp-table [:id "SERIAL" "PRIMARY KEY"])
    (is (db/table? tmp-table))))

;; TODO: test to ensure options applied to column
(deftest add-column-test
  (let [tmp-table "fruit"]
    (db/create-table tmp-table [:id "SERIAL" "PRIMARY KEY"])
    (db/add-column tmp-table "jopotonio" [:integer])))
