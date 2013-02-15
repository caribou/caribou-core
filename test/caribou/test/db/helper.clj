(ns caribou.test.db.helper
  (:use [clojure.test]
        [caribou.util])
  (:require [caribou.db :as db]))

;; zap
(deftest ^:non-db zap-string-test
  (is (= (zap "foobarbaz") "foobarbaz"))
  (is (= (zap "f\\o\"o;b#a%r") "foobar"))
  (is (= (zap "foo'bar") "foo''bar"))
  (is (= (zap :foobarbaz) "foobarbaz"))
  (is (= (zap :foo#bar#baz) "foobarbaz"))
  (is (= (zap 112358) 112358)))

;; clause
(deftest ^:non-db clause-empty-args-test
  (let [clause (clause "foo bar" [])]
    (is (= clause "foo bar"))))

(deftest ^:non-db clause-sql-test
  (let [clause (clause "update %1 set %2 where %3 = '%4'" ["foo", "bar", "baz", "bat"])]
    (is (= clause "update foo set bar where baz = 'bat'"))))

;; TODO: test database...
;; query

(deftest ^:non-db sqlize-test
  (is (= (db/sqlize 1) 1))
  (is (= (db/sqlize true) true))
  (is (= (db/sqlize :foo) "foo"))
  (is (= (db/sqlize "select * from table") "'select * from table'"))
  (is (= (db/sqlize {:foo "bar"}) "'{:foo bar}'")))

(deftest ^:non-db value-map-test
  (is (= (db/value-map {}) ""))
  (is (= (db/value-map {:foo "bar"}) "foo = 'bar'"))
  (is (= (db/value-map {:foo "bar" :baz "bot"}) "foo = 'bar', baz = 'bot'"))
  (is (= (db/value-map {:foo "bar" :baz {:foo "bar"}}) "foo = 'bar', baz = '{:foo bar}'")))
