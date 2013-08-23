(ns caribou.test
  (:require [clojure.java.io :as io]
            [caribou.config :as config]
            [caribou.core :as core]))

(defn read-config
  [db]
  (-> (str "config/test-" (name db) ".clj")
      io/resource
      config/read-config
      core/init))

;; potential where syntax

{:where {:yellow 5 :green 11}}                 ;; WHERE yellow = 5 AND green = 11
{:where {:yellow 5 :green {:> 11}}}            ;; WHERE yellow = 5 AND green > 11
{:where [{:yellow 5} {:green {:red {:= 11}}}]} ;; WHERE yellow = 5 OR green.red = 11
{:where {:! {:yellow 5 :green [1 2 3 4 5]}}}        ;; WHERE yellow = 5 AND green IN (1, 2, 3, 4, 5)
{:where [{:! {:skin "rubbery"}} {:ninja [{:name "Brian"} {:size "Big" :name "Plutonian"}]}]}
