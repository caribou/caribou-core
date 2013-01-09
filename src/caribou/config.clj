(ns caribou.config
  (:use [caribou.debug]
        [clojure.walk :only (keywordize-keys)]
        [caribou.util :only (map-vals pathify file-exists?)])
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [caribou.util :as util]
            [caribou.db.adapter :as db-adapter]
            [caribou.db.adapter.protocol :as adapter]
            [caribou.logger :as logger]))

(import java.net.URI)
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
       "development")))

(defn app-value-eq
  [kw value]
  (= (@app kw) value))

(defn assoc-subname
  [config]
  (adapter/build-subname @db-adapter config))

(def ^{:private true :doc "Map of schemes to subprotocols"} subprotocols
  {"postgres" "postgresql"})

(defn- parse-properties-uri [^URI uri]
  (let [host (.getHost uri)
        port (if (pos? (.getPort uri)) (.getPort uri))
        path (.getPath uri)
        scheme (.getScheme uri)]
    (merge
     {:subname (if port
                 (str "//" host ":" port path)
                 (str "//" host path))
      :subprotocol (subprotocols scheme scheme)}
     (if-let [user-info (.getUserInfo uri)]
             {:user (first (string/split user-info #":"))
              :password (second (string/split user-info #":"))}))))

(defn- strip-jdbc [^String spec]
  (if (.startsWith spec "jdbc:")
    (.substring spec 5)
    spec))

(defn merge-db-creds-from-env
  [db-config user-prop pass-prop]
  (if (and user-prop pass-prop)
    (let [user (System/getProperty user-prop)
          password (System/getProperty pass-prop)]
      (if (and user password)
        (assoc db-config :user user :password password)
        db-config))
    db-config))

(defn configure-db-from-environment
  "Pass in the current config and a map of environment variables that specify where the db connection
  information is stored.  The keys to this map are:
    :connection --> the jdbc connection string
    :username   --> optional username parameter
    :password   --> optional password parameter"
  [config properties]
  (if-let [connection (System/getProperty (:connection properties))]
    (let [connection (strip-jdbc connection)
          uri (URI. connection)
          parsed (parse-properties-uri uri)
          db-config (merge-db-creds-from-env parsed (:username properties) (:password properties))]
      (update-in config [:database] #(merge % db-config)))
    config))

(defn set-db-config
  "Accepts a map to configure the DB.  Format:

    :classname org.postgresql.Driver
    :subprotocol postgresql
    :host localhost
    :database caribou
    :user postgres"
  [db-map]
  (alter db merge (assoc-subname db-map)))

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
    (logger/init logging-config)
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
