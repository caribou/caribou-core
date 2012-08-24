(ns ^{:skip-wiki true}
  caribou.migration
  (:use caribou.debug)
  (:require [clojure.set :as set]
            [clojure.java.jdbc :as sql]
            [clojure.string :as string]
            [caribou.db :as db]
            [caribou.config :as config]
            [caribou.migrations.premigrations :as premigrations]
            [caribou.model :as model]
            [caribou.util :as util]))

(def migrate (fn [] :nothing))

(def premigration-list
  ["create_base_tables"])

(def migration-list
  (ref ["create_models" "create_locked_models" "add_links"]))

(defn push-migration [name]
  (dosync (alter migration-list #(cons name %))))

(defn migration-names []
  (map #(% :name) (db/query "select * from migration")))

(defn migrations-to-run []
  (set/difference @migration-list (migration-names)))

(defn load-user-migrations [path]
  (util/load-path path (fn [file filename]
    (load-file (.toString file))
    (db/insert :migration {:name filename}))))

(defn run-migration [migration]
  (load-file (str "caribou/migrations/" migration ".clj"))
  (db/insert :migration {:name migration}))

(defn run-migrations [config]
  (try
    (let [blank (sql/with-connection config (not (db/table? "migration")))]
      (if blank
    ;; (sql/with-connection config
    ;;   (if (not (db/table? "migration"))
        (do
          (sql/with-connection config
            (premigrations/migrate))
          (sql/with-connection config
            (premigrations/build-models)))))
        ;; (doall (map run-migration premigration-list)))
      ;; (doall (map #(if (not (some #{%} (migration-names))) (run-migration %))
      ;;             @migration-list))
    (sql/with-connection config
      (load-user-migrations "app/migrations"))
    (catch Exception e
      (println "Caught an exception attempting to run migrations: " (.getMessage e) (.printStackTrace e)))))

(defn add-primary-keys
  []
  (try
    (model/init)
    (model/db
     (fn []
       (doseq [model (vals @model/models)]
         (db/add-primary-key (:slug model) "id"))))
    (catch Exception e
      (util/render-exception e))))

