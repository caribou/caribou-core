(ns caribou.config
  (:use [caribou.debug]
        [clojure.walk :only (keywordize-keys)]
        [clojure.string :only (join)]
        [caribou.util :only (map-vals pathify file-exists?)])
  (:require [clojure.java.io :as io]
            [caribou.util :as util]))

(declare boot-file)
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

(defn read-config
  [config-file]
  (with-open [fd (java.io.PushbackReader.
                  (io/reader (io/file config-file)))]
    (read fd)))

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
  (let [boot-file-name "config/boot.clj"
        project-level-boot-file boot-file-name
        parent-level-boot-file (util/pathify [".." boot-file-name])]
    (log :config parent-level-boot-file)
    (def boot-file
      (cond
        (file-exists? project-level-boot-file) project-level-boot-file
        (file-exists? parent-level-boot-file) parent-level-boot-file
        :else nil))
   
    (if (nil? boot-file)
      (throw (Exception. (format "Could not find %s at project or parent level" boot-file-name))))

    (load-file boot-file)))
