(ns caribou.test.index
  (:use [clojure.test])
  (:require [caribou.model :as model]
            [caribou.db :as db]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as sql]
            [caribou.query :as query]
            [caribou.logger :as log]
            [caribou.index :as index]
            [caribou.config :as config]))

(defn remove-models
  []
  (doseq [slug [:cheese]]
    (when (db/table? slug) (model/destroy :model (-> @model/models slug :id)))))

(defn test-init
  []
  (index/purge)
  (model/invoke-models)
  (query/clear-queries)
  (remove-models))

(defn make-models
  []
  (model/create :model {:name "Cheese"
                        :description "Yellowy goodness"
                        :fields [{:name "Name" :type "string" :searchable true}
                                 {:name "Description" :type "text" :searchable true}
                                 {:name "Secret" :type "text" :searchable false}]}))

(defn db-fixture
  [f]
  (sql/with-connection @config/db
    (test-init)
    (make-models)
    (f)
    (remove-models)))    

(defn smoke-test []
  (testing "Smoke?"
    (is (= true true))))

(defn add-to-index-test []
  (let [fromage (model/create :cheese 
                  {:name "Cheddar"
                   :description "Yellow or orange, sharp or not, always good."
                   :secret "Bing!"})
        found (index/search (@model/models :cheese) "cheddar")
        secret (index/search (@model/models :cheese) "bing")
        not-found (index/search (@model/models :cheese) "klaxxon")]
    (testing "New added content's fields are indexed correctly"
      (is (= (count found) 1))
      (is (= (count secret) 0))
      (is (= (count not-found) 0)))))

(defn remove-from-index-test []
  (let [fromage (model/create :cheese
                  {:name "Gruyère"
                   :description "Yum fondue, yeah"
                   :secret "Crunchy frog"})
        found (index/search (@model/models :cheese) "fondue")
        secret (index/search (@model/models :cheese) "frog")
        removed (model/destroy :cheese (:id fromage))
        not-found (index/search (@model/models :cheese) "fondue")]
    (testing "Indexed content is removed when model is deleted"
      (is (= (count found) 1))
      (is (= (count secret) 0))
      (is (= (count not-found) 0)))))

(deftest ^:mysql
  mysql-tests
  (let [config (config/read-config (io/resource "config/test-mysql.clj"))]
    (config/configure config)
    (db-fixture smoke-test)
    (db-fixture remove-from-index-test)
    (db-fixture add-to-index-test))) 

(deftest ^:postgres
  postgres-tests
  (let [config (config/read-config (io/resource "config/test-postgres.clj"))]
    (config/configure config)
    (db-fixture smoke-test)
    (db-fixture remove-from-index-test)
    (db-fixture add-to-index-test)))

(deftest ^:h2
  h2-tests
  (let [config (config/read-config (io/resource "config/test-h2.clj"))]
    (config/configure config)
    (db-fixture smoke-test)
    (db-fixture remove-from-index-test)
    (db-fixture add-to-index-test)))
