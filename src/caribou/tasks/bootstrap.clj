(ns caribou.tasks.bootstrap
  (:require [clojure.java.jdbc :as sql]
            [caribou.db :as db]
            [caribou.config :as config]
            [caribou.migration :as mm]))

(defn bootstrap [config]
  (db/rebuild-database config)
  (mm/run-migrations config))

(defn -main [config-file]
  (bootstrap (config/read-config config-file)))