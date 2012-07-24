(ns caribou.config
  (:use [caribou.debug]
        [clojure.walk :only (keywordize-keys)]
        [clojure.string :only (join)]
        [caribou.util :only (map-vals pathify file-exists?)])
  (:require [clojure.java.io :as io]
            [caribou.util :as util]
            [caribou.db.adapter :as db-adapter]
            [caribou.db.adapter.protocol :as adapter]))

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
  (keyword (or (system-property :environment) "development")))

(defn app-value-eq
  [kw value]
  (= (@app kw) value))

(defn assoc-subname
  [config]
  (adapter/build-subname @db-adapter config))

(defn set-db-config
  "Accepts a map to configure the DB.  Format:

    :classname org.postgresql.Driver
    :subprotocol postgresql
    :host localhost
    :database caribou
    :user postgres"
  [db-map]
  (dosync
   (alter db merge (assoc-subname db-map))))

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
  (let [db-config (config-map :database)]
    (load-caribou-properties)
    (dosync
     (ref-set db-adapter (db-adapter/adapter-for db-config)))
    (dosync
     (alter app merge config-map))
    (dosync
     (alter db merge (assoc-subname db-config)))
    (adapter/init @db-adapter)
    config-map))

(defn init
  []
  (let [boot-resource "config/boot.clj"
        boot (io/resource boot-resource)]

    (if (nil? boot)
      (throw (Exception. (format "Could not find %s on the classpath" boot-resource))))

    (load-reader (io/reader boot))))

