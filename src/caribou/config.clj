(ns caribou.config
  (:use [caribou.debug]
        [clojure.walk :only (keywordize-keys)]
        [clojure.string :only (join)]
        [caribou.util :only (map-vals pathify file-exists?)])
  (:require [clojure.java.io :as io]
            [caribou.util :as util]))

(declare config-file)
(def get-config)

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

(defn init
  []
  (let [config-file-name "caribou.clj"
        project-level-config-file config-file-name
        parent-level-config-file (util/pathify (concat [".."] config-file-name))]
    (def config-file
      (cond
        (file-exists? project-level-config-file) project-level-config-file
        (file-exists? parent-level-config-file) parent-level-config-file
        :else nil))
   
    (if (nil? config-file)
      (throw (Exception. (format "Could not find %s at project or parent level" config-file-name))))

    (load-file config-file)))
