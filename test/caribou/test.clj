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

