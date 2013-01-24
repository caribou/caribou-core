(ns caribou.migration
  (:require [clojure.set :as set]
            [clojure.java.jdbc :as sql]
            [caribou.util :as util]
            [caribou.db :as db]))

(defn app-migration-namespace []
  ;; htf do we know the name of the current project?
  (let [project-name (last (clojure.string/split (System/getProperty "user.dir") (re-pattern util/file-separator)))]
    (str project-name ".migrations")))

(defn used-migrations
  []
  (try 
    (map #(% :name) (util/query "select * from migration"))
    (catch Exception e
      (println (.getMessage e)))))

(defn load-migration-order
  [namespace]
  (let [order-namespace (symbol (str namespace ".order"))
        _               (require :reload order-namespace)
        order-symbol    (ns-resolve order-namespace (symbol "order"))]
        (map #(str namespace "." %) @order-symbol)))

(defn run-migration [migration]
  (println "Running migration " migration)
  (let [ns (symbol migration)
        _  (require :reload ns)
        migrate-symbol (ns-resolve ns (symbol "migrate"))]
    (migrate-symbol)
    (db/insert :migration {:name migration})))

(defn run-migrations
  [db-config]
  (println db-config)
  (sql/with-connection db-config
    (let [core-migrations (load-migration-order "caribou.migrations")
          app-migrations  () ; (load-migration-order (app-migration-namespace))
          all-migrations  (concat core-migrations app-migrations)
          _ (println core-migrations (used-migrations))
          unused-migrations (set/difference (set all-migrations) (set (used-migrations)))]
      (doseq [m unused-migrations]
        (run-migration m)))))
