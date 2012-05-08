(ns caribou.db.adapter.protocol)

(defprotocol DatabaseAdapter
  (table? [this table])
  (insert-result [this table result]))