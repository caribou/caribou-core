(ns caribou.config
  (:use [caribou.debug]
        [clojure.walk :only (keywordize-keys)]
        [clojure.string :only (join)]
        [caribou.util :only (map-vals pathify file-exists?)])
  (:require [clojure.java.io :as io]
            [caribou.util :as util]))

(def app (ref {}))
(def db (ref {}))

(defn app-value-eq
  [kw value]
  (= (@app kw) value))

(defn assoc-subname
  [config]
  (let [host (or (config :host) "localhost")
        subname (or (config :subname) (str "//" host "/" (config :database)))]
    (assoc config :subname subname)))

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

(defn configure
  [config-map]
  (dosync
    (alter app merge config-map))
  ; save just the DB config by itself for convenience
  (dosync
    (alter db merge (assoc-subname (config-map :database))))
  config-map)

(defn caribou-home
  []
  (pathify [(System/getProperty "user.home") ".caribou"]))
