(ns caribou.db.adapter.protocol)

(defprotocol DatabaseAdapter
  (init [this])
  (table? [this table])
  (build-subname [this config])
  (insert-result [this table result])
  (text-value [this text]))