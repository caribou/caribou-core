(ns caribou.db.adapter.protocol)

(defprotocol DatabaseAdapter
  (init [this])
  (table? [this table])
  (build-subname [this config])
  (insert-result [this table result])
  (rename-column [this table column new-name])
  (set-required [this table column value])
  (text-value [this text]))