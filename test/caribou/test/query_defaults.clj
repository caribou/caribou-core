(ns caribou.test.query-defaults
  (:use [clojure.test])
  (:require [clojure.java.jdbc :as sql]
            [clojure.java.io :as io]
            [caribou.db :as db]
            [caribou.model :as model]
            [caribou.config :as config]
            [clojure.pprint :as pprint]))

(def test-config (config/read-config (io/resource "config/test.clj")))
(config/configure test-config)

(deftest add-query-defaults
	(let [defaults {:name "Tarquin" :status "Biscuit-barrel"}
        test-query {:where {:bus-stop "f'tang f'tang"}
                    :order {:slug :asc}
                    :limit 10 :offset 3}
        new-query (model/apply-query-defaults test-query defaults)
        expected-query {:where {:name "Tarquin" :bus-stop "f'tang f'tang" :status "Biscuit-barrel"}
                      :order {:slug :asc}
                      :limit 10 :offset 3}
       ]
    (is (= new-query expected-query))))

(deftest expanded-query-defaults-no-includes
  (let [defaults {:name "Mr. Creosote"}
        test-query {:where {:full true}}
        expanded-defaults (model/expanded-query-defaults test-query defaults)
        ]
    (is (= expanded-defaults {:name "Mr. Creosote"}))))

(deftest expanded-query-defaults-no-includes-nested-where
  (let [defaults {:popular true}
        test-query {:where {:organization {:name "Judean People's Front"}}}
        expanded-defaults (model/expanded-query-defaults test-query defaults)
        expected-defaults {:popular true :organization {:popular true}}
        ]
    (is (= expanded-defaults expected-defaults))))

(deftest expanded-query-defaults
  (let [defaults {:name "Mr. Creosote"}
        test-query {:include {:wafer-thin-mint {} :bucket {:throw-up {}}}
                    :where {:slug "name"}}
        expanded-defaults (model/expanded-query-defaults test-query defaults)
        expected-defaults {:name "Mr. Creosote"
                           :wafer-thin-mint {:name "Mr. Creosote"}
                           :bucket {:name "Mr. Creosote" :throw-up {:name "Mr. Creosote"}}}
        _ (pprint/pprint expanded-defaults)
        _ (pprint/pprint expected-defaults)
        ]
    (is (= expanded-defaults expected-defaults))))

(deftest expanded-query-defaults-nested
  (let [defaults {:status "Awesome"}
        test-query {:include {:french-waiter {:wafer-thin-mint {}} :bucket {:throw-up {}}}
                    :where {:slug "name"}}
        expanded-defaults (model/expanded-query-defaults test-query defaults)
        expected-defaults {:status "Awesome"
                           :french-waiter {:status "Awesome" :wafer-thin-mint {:status "Awesome"}}
                           :bucket {:status "Awesome" :throw-up {:status "Awesome"}}}
        ]
    (is (= expanded-defaults expected-defaults))))


(deftest expanded-query-defaults-stomp-on-query
  (let [defaults {:status "Fishy"}
        test-query {:include {:french-waiter {}}
                    :where {:status "Silly" :french-waiter {:status "Oily"}}}
        expanded-defaults (model/expanded-query-defaults test-query defaults)
        expected-defaults {:status "Fishy"
                           :french-waiter {:status "Fishy"}}
        ]
    (is (= expanded-defaults expected-defaults))))



