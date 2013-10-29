(ns caribou.test.gather-helpers
  (:use clojure.test
        caribou.gather))

(deftest smoke-test
  (testing "if things are sane"
    (is (= 1 1))))

(deftest where-clause
  (testing "where clause addition"
    (is (-> (query)
            (where {:a "foo"}))
        {:where {:a "foo"}})))

(deftest where-clauses
  (testing "many where clauses"
    (is (-> (query)
            (where {:a "this"})
            (where {:b "that"})
            (where {:c {:d "another"}}))
        {:where {:a "this" :b "that" :c {:d "another"}}})))

(deftest limit-clause
  (testing "adding a limit"
    (is (-> (query)
            (where {:a "foo"})
            (limit 10))
        {:where {:a "foo"}
         :limit 10})))

(deftest offset-clause
  (testing "adding an offset"
    (is  (-> (query)
             (where {:a "foo"})
             (offset 10))
         {:where {:a "foo"}
          :offset 10})))

(deftest include-clauses
  (testing "adding includes"
    (is (-> (query)
            (where {:a "foo"})
            (include {:this {}})
            (include {:that {:another {}}}))
        {:where {:a "foo"}
         :include {:this {} :that {:another {}}}})))

(deftest order-by-clause
  (testing "adding an order-by clause"
    (is (-> (query :thing)
            (where {:a "foo"})
            (order-by {:this :asc})
            (include {:that {:another {}}}))
        {:model :thing
         :where {:a "foo"}
         :include {:that {:another {}}}
         :order {:this :asc}})))

(deftest order-clause
  (testing "adding an order clause"
    (is (-> (query)
            (where {:a "foo"})
            (include {:thing {}})
            (order {:foo :asc}))
        {:where {:a "foo"}
         :include {:thing {}}
         :order {:foo :asc}})))

(deftest where-key-values
  (testing "adding a where clause as a key-path/value works"
    (is (-> (query)
            (where :a "foo")
            (where [:b :c :e] "bar"))
        {:where {:a "foo" :b {:c {:e "bar"}}}})))

(deftest where-clause
  (testing "where clause addition"
    (is (-> (query)
            (where {:a "foo"}))
        {:where {:a "foo"}})))

(deftest where-clauses
  (testing "many where clauses"
    (is (-> (query)
            (where {:a "this"})
            (where {:b "that"})
            (where {:c {:d "another"}}))
        {:where {:a "this" :b "that" :c {:d "another"}}})))

(deftest limit-clause
  (testing "adding a limit"
    (is (-> (query)
            (where {:a "foo"})
            (limit 10))
        {:where {:a "foo"}
         :limit 10})))

(deftest offset-clause
  (testing "adding an offset"
    (is  (-> (query)
             (where {:a "foo"})
             (offset 10))
         {:where {:a "foo"}
          :offset 10})))

(deftest include-clauses
  (testing "adding includes"
    (is (-> (query)
            (where {:a "foo"})
            (include {:this {}})
            (include {:that {:another {}}}))
        {:where {:a "foo"}
         :include {:this {} :that {:another {}}}})))

(deftest order-by-clause
  (testing "adding an order-by clause"
    (is (-> (query :thing)
            (where {:a "foo"})
            (order-by {:this :asc})
            (include {:that {:another {}}}))
        {:model :thing
         :where {:a "foo"}
         :include {:that {:another {}}}
         :order {:this :asc}})))

(deftest order-clause
  (testing "adding an order clause"
    (is (-> (query)
            (where {:a "foo"})
            (include {:thing {}})
            (order {:foo :asc}))
        {:where {:a "foo"}
         :include {:thing {}}
         :order {:foo :asc}})))

(deftest where-key-values
  (testing "adding a where clause as a key-path/value works"
    (is (-> (query)
            (where :a "foo")
            (where [:b :c :e] "bar"))
        {:where {:a "foo" :b {:c {:e "bar"}}}})))
