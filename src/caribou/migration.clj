(ns caribou.migration
  (:require [clojure.set :as set]
            [clojure.java.jdbc :as sql]
            [clojure.pprint :as pprint]
            [caribou.util :as util]
            [caribou.config :as config]
            [caribou.db :as db]))

; (defn app-migration-namespace []
;   ;; htf do we know the name of the current project?
;   (let [project-name (last (clojure.string/split (System/getProperty "user.dir") (re-pattern util/file-separator)))]
;     (str project-name ".migrations")))

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

(defn run-migration
  [migration]
  (println "Running migration " migration)
  (let [ns (symbol migration)
        _  (require :reload ns)
        migrate-symbol (ns-resolve ns (symbol "migrate"))]
    (migrate-symbol)
    (db/insert :migration {:name migration})))

(defn run-migrations
  [prj config-file & migration]
  (let [cfg (config/read-config config-file)
        _   (config/configure cfg)
        db-config (config/assoc-subname (cfg :database))
        app-migration-namespace (:migration-namespace prj)]
    (config/configure cfg)

    (println db-config)
    (sql/with-connection db-config
      (let [core-migrations (load-migration-order "caribou.migrations")
            app-migrations  (load-migration-order app-migration-namespace)
            all-migrations  (concat core-migrations app-migrations)
            _ (println all-migrations (used-migrations))
            unused-migrations (set/difference (set all-migrations) (set (used-migrations)))]
        (doseq [m unused-migrations]
          (run-migration m))))))

;; starting to add rollbacks, this is a bit hazy right now
(defn run-rollback
  [rollback]
  (println "Trying to run rollback " rollback)
  (let [used-migrations (used-migrations)]
    (if-not (= (last used-migrations) rollback)
      (throw (Exception. "You can only roll back the last-applied migration"))
      (do
        (let [ns (symbol rollback)
              _  (require :reload ns)
              rollback-symbol (ns-resolve ns (symbol "rollback"))]
        (rollback-symbol)
        (when (db/table? "migration")
          (db/delete :migration "name = '%1'" rollback)))))))


(defn run-rollbacks
  [prj config-file & rollback]
  (let [cfg (config/read-config config-file)
        _   (config/configure cfg)
        db-config (config/assoc-subname (cfg :database))
        app-migration-namespace (:migration-namespace prj)]
    (config/configure cfg)

    (println db-config)
    (sql/with-connection db-config
      (let [available-rollbacks (reverse (used-migrations))]
        (doseq [r available-rollbacks]
          (run-rollback r))))))
