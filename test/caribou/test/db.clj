(ns caribou.test.db
  (:use [clojure.test]
        [caribou.util]
        [caribou.debug])
  (:require [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]
            [caribou.config :as config]
            [caribou.db :as db]))

(def test-config (config/read-config (io/resource "config/test.clj")))
(config/configure test-config)

;; zap
(deftest zap-string-test
  (is (= (zap "foobarbaz") "foobarbaz"))
  (is (= (zap "f\\o\"o;b#a%r") "foobar"))
  (is (= (zap "foo'bar") "foo''bar"))
  (is (= (zap :foobarbaz) "foobarbaz"))
  (is (= (zap :foo#bar#baz) "foobarbaz"))
  (is (= (zap 112358) 112358)))

;; clause
(deftest clause-empty-args-test
  (let [clause (clause "foo bar" [])]
    (is (= clause "foo bar"))))

(deftest clause-sql-test
  (let [clause (clause "update %1 set %2 where %3 = '%4'" ["foo", "bar", "baz", "bat"])]
    (is (= clause "update foo set bar where baz = 'bat'"))))

;; TODO: test database...
;; query
(deftest query-test
  (sql/with-connection @config/db
    (let [q (query "select * from model")]
      (is (not (empty? q))))))

(deftest query2-exception-test
  (sql/with-connection @config/db
    (try 
      (query "select * from modelz")
      (catch Exception e 
        (is (instance? java.lang.Exception e))))))

(deftest sqlize-test
  (is (= (db/sqlize 1) 1))
  (is (= (db/sqlize true) true))
  (is (= (db/sqlize :foo) "foo"))
  (is (= (db/sqlize "select * from table") "'select * from table'"))
  (is (= (db/sqlize {:foo "bar"}) "'{:foo bar}'")))

(deftest value-map-test
  (is (= (db/value-map {}) ""))
  (is (= (db/value-map {:foo "bar"}) "foo = 'bar'"))
  (is (= (db/value-map {:foo "bar" :baz "bot"}) "foo = 'bar', baz = 'bot'"))
  (is (= (db/value-map {:foo "bar" :baz {:foo "bar"}}) "foo = 'bar', baz = '{:foo bar}'")))

;; TODO: insert / update / delete / fetch

(deftest choose-test
  (sql/with-connection @config/db
    (is (= (get (db/choose :model 1) :name) "Model"))
    (is (= (get (db/choose :model 0) :name) nil))))

(deftest table-test
  (sql/with-connection @config/db
    (is (db/table? "model"))
    (is (not (db/table? "modelzzzz")))))

(deftest create-new-table-drop-table-test
  (sql/with-connection @config/db
    (let [tmp-table "veggies"]
      (try
        (do
          (db/create-table tmp-table [:id "SERIAL" "PRIMARY KEY"])
          (is (db/table? tmp-table)))
        (catch Exception e (log "create-table-test" "Attemptng to create a table that already exists"))
        (finally (db/drop-table tmp-table))))))

;; TODO: test to ensure options applied to column
(deftest add-column-test
  (sql/with-connection @config/db
    (let [tmp-table "fruit"]
      (try
        (do
          (db/create-table tmp-table [:id "SERIAL" "PRIMARY KEY"])
          (db/add-column tmp-table "jopotonio" [:integer]))
        (catch Exception e (render-exception e))
        (finally (db/drop-table tmp-table))))))

