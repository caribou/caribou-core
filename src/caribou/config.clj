(ns caribou.config
  (:use [caribou.debug]
        [clojure.walk :only (keywordize-keys)]
        [clojure.string :only (join)]
        [caribou.util :only (map-vals pathify file-exists?)])
  (:require [clojure.java.io :as io]
            [caribou.util :as util]
            [caribou.db.adapter :as db-adapter]
            [caribou.db.adapter.protocol :as adapter]
            [caribou.logger :as logger]))


(declare config-path)

(def app (ref {}))
(def db (ref {}))
(def db-adapter (ref nil))

(defn set-properties
  [props]
  (doseq [prop-key (keys props)]
    (System/setProperty (name prop-key) (str (get props prop-key)))))

(defn load-caribou-properties
  []
  (let [props (util/load-props "caribou.properties")]
    (set-properties props)))

(defn system-property
  [key]
  (.get (System/getProperties) (name key)))

(defn environment
  []
  (keyword
   (or (system-property :environment)
       (system-property :PARAM1)
       "development")))

(defn app-value-eq
  [kw value]
  (= (@app kw) value))

(defn assoc-subname
  [config]
  (if-let [jdbc-connection (system-property :JDBC_CONNECTION_STRING)]
    (let [pattern (re-pattern (str "jdbc:" (:subprotocol config) ":(.+)"))]
      (if-let [subname (last (re-find pattern jdbc-connection))]
        (assoc config :subname subname)
        (adapter/build-subname @db-adapter config)))
    (adapter/build-subname @db-adapter config)))

(defn set-db-config
  "Accepts a map to configure the DB.  Format:

    :classname org.postgresql.Driver
    :subprotocol postgresql
    :host localhost
    :database caribou
    :user postgres"
  [db-map]
  (let [user (or (system-property :DATABASE_USERNAME) (:user db-map))
        password (or (system-property :DATABASE_PASSWORD) (:password db-map))
        db-map (assoc db-map :user user :password password)]
    (dosync
     (alter db merge (assoc-subname db-map)))))

(defn read-config
  [config-file]
  (with-open [fd (java.io.PushbackReader.
                  (io/reader (io/file config-file)))]
    (read fd)))

(defn read-database-config
  [config-file]
  (assoc-subname ((read-config config-file) :database)))

(defn configure
  [config-map]
  (let [db-config (config-map :database)
        logging-config (config-map :logging)]
    (dosync
     (ref-set db-adapter (db-adapter/adapter-for db-config)))
    (dosync
     (alter app merge config-map))
    (dosync
     (alter db merge (assoc-subname db-config)))
    (adapter/init @db-adapter)
    (dosync
     (alter logger/defaults merge logging-config))
    (logger/init)
    config-map))

(defn init
  []
  (load-caribou-properties)
  (let [boot-resource "config/boot.clj"
        boot (io/resource boot-resource)]

    (if (nil? boot)
      (throw (Exception.
              (format "Could not find %s on the classpath" boot-resource))))
    (load-reader (io/reader boot))))

