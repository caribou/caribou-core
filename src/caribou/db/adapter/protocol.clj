(ns caribou.db.adapter.protocol)

(defprotocol DatabaseAdapter
  (init [this])
  (table? [this table])
  (build-subname [this config])
  (insert-result [this table result])
  (rename-clause [this])
  (text-value [this text]))