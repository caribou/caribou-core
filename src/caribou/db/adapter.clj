(ns caribou.db.adapter
  (:use caribou.db.adapter.postgres caribou.db.adapter.h2)
  (:import [caribou.db.adapter.postgres PostgresAdapter]
           [caribou.db.adapter.h2 H2Adapter]))

(defn adapter-for
  [config]
  (condp = (config :subprotocol)
    "postgresql" (PostgresAdapter. config)
    "h2" (H2Adapter. config)))