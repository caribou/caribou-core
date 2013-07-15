(ns caribou.test.position-fields
  (:require [caribou
             [model :as model]
             [debug :as debug]
             [test :as ctest]
             [core :as core]
             [query :as query]
             [db :as db]
             [config :as config]]
            [clojure
             [test :as test
              :refer [testing is deftest]]]))

(defn db-fixture
  [f]
  (doseq [db ["mysql" "postgres" "h2"]]
    (core/with-caribou
      (assoc-in (ctest/read-config db) [:logging :loggers 0 :level] :warn)
      (model/invoke-models)
      (query/clear-queries)
      (f)
      (doseq [slug [:yellow]]
        (when (db/table? slug)
          (model/destroy :model (model/models slug :id)))))))

(test/use-fixtures :each db-fixture)

(deftest position-field
  (model/create :model {:name "YELLOW" :fields [{:type "string" :name "ADJ"}]})
  (doseq [adj ["a" "b" "c" "d" "e" "f" "g"]]
    (model/create :yellow {:adj adj}))
  (is (= [[1 0 "a"] [2 1 "b"] [3 2 "c"] [4 3 "d"] [5 4 "e"] [6 5 "f"] [7 6 "g"]]
         (mapv (juxt :id :position :adj) (model/gather :yellow))))
  (model/update :yellow 1 {:adj "a'"})
  (is (= [[1 0 "a'"] [2 1 "b"] [3 2 "c"] [4 3 "d"] [5 4 "e"] [6 5 "f"] [7 6 "g"]]
         (mapv (juxt :id :position :adj) (model/gather :yellow)))))
