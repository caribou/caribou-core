(ns caribou.test.query-defaults
  (:use [clojure.test])
  (:require [caribou.query :as query]
            [caribou.model :as model]))

(deftest ^:non-db add-query-defaults
	(let [defaults {:name "Tarquin" :status "Biscuit-barrel"}
              test-query {:where {:bus-stop "f'tang f'tang"}
                          :order {:slug :asc}
                          :limit 10 :offset 3}
              new-query (query/apply-query-defaults test-query defaults)
              expected-query {:where {:name "Tarquin"
                                      :bus-stop "f'tang f'tang"
                                      :status "Biscuit-barrel"}
                              :order {:slug :asc}
                              :limit 10 :offset 3}]
          (is (= new-query expected-query))))

(deftest ^:non-db expanded-query-defaults-no-includes
  (let [defaults {:name "Mr. Creosote"}
        test-query {:where {:full true}}
        expanded-defaults (query/expanded-query-defaults test-query defaults)]
    (is (= expanded-defaults {:name "Mr. Creosote"}))))

(deftest ^:non-db expanded-query-defaults-no-includes-nested-where
  (let [defaults {:popular true}
        test-query {:where {:organization {:name "Judean People's Front"}}}
        expanded-defaults (query/expanded-query-defaults test-query defaults)
        expected-defaults {:popular true :organization {:popular true}}]
    (is (= expanded-defaults expected-defaults))))

(deftest ^:non-db expanded-query-defaults
  (let [defaults {:name "Mr. Creosote"}
        test-query {:include {:wafer-thin-mint {} :bucket {:throw-up {}}}
                    :where {:slug "name"}}
        expanded-defaults (query/expanded-query-defaults test-query defaults)
        expected-defaults {:name "Mr. Creosote"
                           :wafer-thin-mint {:name "Mr. Creosote"}
                           :bucket {:name "Mr. Creosote"
                                    :throw-up {:name "Mr. Creosote"}}}]
    (is (= expanded-defaults expected-defaults))))

(deftest ^:non-db expanded-query-defaults-nested
  (let [defaults {:status "Awesome"}
        test-query {:include {:french-waiter {:wafer-thin-mint {}}
                              :bucket {:throw-up {}}}
                    :where {:slug "name"}}
        expanded-defaults (query/expanded-query-defaults test-query defaults)
        expected-defaults {:status "Awesome"
                           :french-waiter {:status "Awesome"
                                           :wafer-thin-mint {:status "Awesome"}}
                           :bucket {:status "Awesome"
                                    :throw-up {:status "Awesome"}}}]
    (is (= expanded-defaults expected-defaults))))


(deftest ^:non-db expanded-query-defaults-stomp-on-query
  (let [defaults {:status "Fishy"}
        test-query {:include {:french-waiter {}}
                    :where {:status "Silly" :french-waiter {:status "Oily"}}}
        expanded-defaults (query/expanded-query-defaults test-query defaults)
        expected-defaults {:status "Fishy"
                           :french-waiter {:status "Fishy"}}]
    (is (= expanded-defaults expected-defaults))))

