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

(defn update-in-index-test []
  (let [fromage (model/create :cheese
                  {:name "Camembert"
                   :description "A bit runny"
                   :secret "shhh"})
        found (index/search (@model/models :cheese) "runny")
        secret (index/search (@model/models :cheese) "shhh")
        updated (model/update :cheese (:id fromage) (assoc fromage :description "The cat's eaten it"))
        not-found (index/search (@model/models :cheese) "runny")
        now-found (index/search (@model/models :cheese) "eaten")]
    (testing "Updated records are updated in index"
      (is (= (count found) 1))
      (is (= (count secret) 0))
      (is (= (count not-found) 0))
      (is (= (count now-found) 1)))))

(defn add-localized-to-index-test []
  (let [cheese-model (@model/models :cheese)
        localized (model/update :cheese (:id cheese-model) {:localized true})
        spanish (model/create :locale {:description "Spanish" :code "es_ES" :region "Spain"})
        argentine (model/create :locale {:description "Argentine Spanish" :code "es_AR" :region "Argentina"})
        queso (model/create :cheese
                {:name "Idiazabal"
                 :description "Queso vasco"
                 :secret "Xabi Alonso!"}
                {:locale "es_ES"})
        provoleta (model/create :cheese
                {:name "Provoleta"
                 :description "Queso asado en el estilo argentino"
                 :secret "Messi!"}
                {:locale "es_AR"})
        found-queso (index/search cheese-model "vasco")
        found-provoleta (index/search cheese-model "argentino")
        not-found-queso (index/search cheese-model "vasco" {:locale "es_AR"})
        not-found-provoleta (index/search cheese-model "argentino" {:locale "es_ES"})
        both-found (index/search cheese-model "queso")
        neither-found (index/search cheese-model "messi OR alonso")
        neither-found-locale (index/search cheese-model "queso" {:locale "en_UK"})]
    (testing "No clash between indexed records of different locales"
      (is (= (count found-queso) 1))
      (is (= (count found-provoleta) 1))
      (is (= (count not-found-queso) 0))
      (is (= (count not-found-provoleta) 0))
      (is (= (count both-found) 2))
      (is (= (count neither-found) 0))
      (is (= (count neither-found-locale) 0)))))

(defn single-localized-record-test []
  (let [cheese-model (@model/models :cheese)
        localized (model/update :cheese (:id cheese-model) {:localized true})
        german (model/create :locale {:description "Swiss German" :code "de_CH" :region "Switzerland"})
        french (model/create :locale {:description "Swiss French" :code "fr_CH" :region "Switzerland"})
        kaese (model/create :cheese 
               {:name "Ausgezeichnet Blumenfahrtverzeichnis"
                :description "Überstraßenbahn Fahrvergnügen"
                :secret "Hasslehoff!"}
               {:locale "de_CH"})
        fromage (model/update :cheese (:id kaese)
               {:name "Les baricades misterieuses"
                :description "La plume de ma tante est dans le jardin de mon oncle"
                :secret "Asterix!"}
               {:locale "fr_CH"})
        found-kaese (index/search cheese-model "Fahrvergnügen")
        found-fromage (index/search cheese-model "baricades")
        not-found-kaese (index/search cheese-model "tante" {:locale "de_CH"})
        not-found-fromage (index/search cheese-model "ausgezeichnet" {:locale "fr_CH"})
        neither-found (index/search cheese-model "hasslehoff OR asterix")
        neither-found-locale (index/search cheese-model "plume OR blumenfahrtverzeichnis" {:locale "de_AT"})]
    (testing "Single localized record"
      (is (= (count found-kaese) 1))
      (is (= (count found-fromage) 1))
      (is (= (:id found-kaese) (:id found-fromage)))
      (is (= (count not-found-kaese) 0))
      (is (= (count not-found-fromage) 0))
      (is (= (count neither-found) 0))
      (is (= (count neither-found-locale) 0)))))


(deftest ^:mysql
  mysql-tests
  (let [config (config/read-config (io/resource "config/test-mysql.clj"))]
    (config/configure config)
    (db-fixture smoke-test)
    (db-fixture add-to-index-test)
    (db-fixture remove-from-index-test)
    (db-fixture update-in-index-test)
    (db-fixture add-localized-to-index-test)
    )) 

(deftest ^:postgres
  postgres-tests
  (let [config (config/read-config (io/resource "config/test-postgres.clj"))]
    (config/configure config)
    (db-fixture smoke-test)
    (db-fixture add-to-index-test)
    (db-fixture remove-from-index-test)
    (db-fixture update-in-index-test)
    (db-fixture add-localized-to-index-test)
    ))

(deftest ^:h2
  h2-tests
  (let [config (config/read-config (io/resource "config/test-h2.clj"))]
    (config/configure config)
    (db-fixture smoke-test)
    (db-fixture add-to-index-test)
    (db-fixture remove-from-index-test)
    (db-fixture update-in-index-test)
    (db-fixture add-localized-to-index-test)
    ))
