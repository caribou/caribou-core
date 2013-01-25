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

(defn symbol-in-namespace
  [sym n]
  (let [ns (symbol n)
        _  (require :reload ns)
        resolved-symbol (ns-resolve ns (symbol sym))]
    resolved-symbol))


(defn load-migration-order
  [namespace]
  (let [order-namespace (str namespace ".order")
        order-symbol (symbol-in-namespace "order" order-namespace)]
    (if-not (nil? order-symbol)
      (map #(str namespace "." %) @order-symbol)
      ())))

(defn run-migration
  [migration]
  (println "Running migration " migration)
  (let [migrate-symbol (symbol-in-namespace "migrate" migration)
        rollback-symbol (symbol-in-namespace "rollback" migration)]
    (when (nil? migrate-symbol)
      (throw (Exception. (str migration " has no 'migrate' function"))))
    (when (nil? rollback-symbol)
      (println (str "WARNING: No rollback available for migration " migration)))
    (migrate-symbol)
    (db/insert :migration {:name migration})))

(defn run-migrations
  [prj config-file & args]
  (let [migrations (first args)
        cfg (config/read-config config-file)
        _   (config/configure cfg)
        db-config (config/assoc-subname (cfg :database))
        app-migration-namespace (:migration-namespace prj)]
    (config/configure cfg)

    (sql/with-connection db-config
      (let [core-migrations (load-migration-order "caribou.migrations")
            app-migrations  (load-migration-order app-migration-namespace)
            all-migrations  (if (empty? migrations)
                              (concat core-migrations app-migrations)
                              migrations)
            unused-migrations (set/difference (set all-migrations) (set (used-migrations)))]
        (doseq [m unused-migrations]
          (run-migration m))))))

;; starting to add rollbacks, this is a bit hazy right now
;; so don't rely on it...
(defn run-rollback
  [rollback]
  (println "Trying to run rollback " rollback)
  (let [used-migrations (used-migrations)]
    (if-not (= (last used-migrations) rollback)
      (do
        (println "You can only roll back the last-applied migration:")
        (pprint/pprint used-migrations)
        false)
      (do
        (let [rollback-symbol (symbol-in-namespace "rollback" rollback)]
          (when (nil? rollback-symbol)
            (throw (Exception. (str rollback " has no 'rollback' function"))))
          (rollback-symbol)
          (when (db/table? "migration")
            (db/delete :migration "name = '%1'" rollback))
          true)))))


(defn run-rollbacks
  [prj config-file & args]
  (let [rollbacks (first args)
        cfg (config/read-config config-file)
        _   (config/configure cfg)
        db-config (config/assoc-subname (cfg :database))
        app-migration-namespace (:migration-namespace prj)]
    (config/configure cfg)

    (println rollbacks)
    (sql/with-connection db-config
      (let [available-rollbacks (if (empty? rollbacks)
                                  (reverse (used-migrations))
                                  rollbacks)]
        (doseq [r available-rollbacks]
          (run-rollback r))))))
