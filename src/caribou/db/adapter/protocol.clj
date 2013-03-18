(ns caribou.db.adapter.protocol)

(defprotocol DatabaseAdapter
  (init [this])
  (unicode-supported? [this])
  (supports-constraints? [this])
  (table? [this table])
  (build-subname [this config])
  (insert-result [this table result])
  (rename-column [this table column new-name])
  (set-required [this table column value])
  (drop-index [this table column])
  (drop-model-index [this old-table new-table column])
  (text-value [this text]))