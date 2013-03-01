(ns caribou.migration
  (:require [clojure.set :as set]
            [clojure.java.jdbc :as sql]
            [clojure.pprint :as pprint]
            [leiningen.core.main :as lein]
            [caribou.util :as util]
            [caribou.config :as config]
            [caribou.logger :as log]
            [caribou.db :as db]))

(defn used-migrations
  []
  (try 
    (map #(% :name) (util/query "select * from migration"))
    (catch Exception e
      (log/error (.getMessage e)))))

(defn symbol-in-namespace
  [sym n]
  (let [namesp (symbol n)
        _  (require :reload namesp)
        resolved-symbol (ns-resolve namesp (symbol sym))]
    resolved-symbol))

(defn load-migration-order
  [namespace]
  (let [order-namespace (str namespace ".order")
        order-symbol (symbol-in-namespace "order" order-namespace)]
    (if-not (nil? order-symbol)
      (map #(str namespace "." %) @order-symbol)
      ())))

(defn munge-for-migrate
  [config]
  (if (= (:subprotocol config) "h2")
    (merge config {:db-path "/./"})
    config))

(defn run-migration
  [migration]
  (log/notice (str " -> migration " migration " started."))
  (let [migrate-symbol (symbol-in-namespace "migrate" migration)
        rollback-symbol (symbol-in-namespace "rollback" migration)]
    (when (nil? migrate-symbol)
      (throw (Exception. (str migration " has no 'migrate' function"))))
    (when (nil? rollback-symbol)
      (log/warn (str "No rollback available for migration " migration)))
    (migrate-symbol)
    (db/insert :migration {:name migration})
    (log/info (str " <- migration " migration " ended."))))

(defn run-migrations
  [prj config-file exit? & migrations]
  ;(pprint/pprint migrations)

  (let [cfg (config/read-config config-file)
        _   (config/configure cfg)
        db-config (config/assoc-subname (munge-for-migrate (cfg :database)))
        app-migration-namespace (:migration-namespace prj)]
    (config/configure cfg)

    (sql/with-connection db-config
      (log/info "Already used these: ")
      (pprint/pprint (used-migrations))
      (let [core-migrations (load-migration-order "caribou.migrations")
            app-migrations  (if app-migration-namespace
                              (load-migration-order app-migration-namespace)
                              (log/warn "no application namespace provided."))
            all-migrations  (if (empty? (remove nil? migrations))
                              (concat core-migrations app-migrations)
                              migrations)
            unused-migrations (set/difference (set all-migrations) (set (used-migrations)))]
        (doseq [m unused-migrations]
          (run-migration m))
        (log/notice " <- run-migrations ended.")))
    ;; This is because the presence of an active h2 thread prevents
    ;; this function from returning to lein-caribou, which invoked
    ;; it using 'eval-in-project'
    (if exit? (lein/exit))))

(defn run-rollback
  [rollback]
  (log/notice (str "Trying to run rollback " rollback))
  (let [used-migrations (used-migrations)]
    (if-not (= (last used-migrations) rollback)
      (do
        (log/error
         (str "You can only roll back the last-applied migration:"
              (with-out-str (pprint/pprint used-migrations))))
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
  [prj config-file exit? & rollbacks]
  ;(pprint/pprint rollbacks)
  (let [cfg (config/read-config config-file)
        _   (config/configure cfg)
        db-config (config/assoc-subname (munge-for-migrate (cfg :database)))
        app-migration-namespace (:migration-namespace prj)]
    (config/configure cfg)

    (sql/with-connection db-config
      (let [available-rollbacks (if (empty? (remove nil? rollbacks))
                                  (reverse (used-migrations))
                                  rollbacks)]
        (doseq [r available-rollbacks]
          (run-rollback r))
        (log/notice " <- run-rollbacks ended.")))
    ;; see comment in run-migrations, above
    (if exit? (lein/exit))))
