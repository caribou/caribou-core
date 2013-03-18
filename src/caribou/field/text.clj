(ns caribou.field.text
  (:require [caribou.field :as field]
            [caribou.util :as util]
            [caribou.config :as config]
            [caribou.db.adapter.protocol :as adapter]
            [caribou.validation :as validation]))

(defrecord TextField [row env]
  field/Field
  (table-additions [this field] [[(keyword field) :text]])
  (subfield-names [this field] [])
  (setup-field [this spec] nil)
  (rename-model [this old-slug new-slug])
  (rename-field [this old-slug new-slug])
  (cleanup-field [this]
    (field/field-cleanup this))
  (target-for [this] nil)
  (update-values [this content values]
    (let [key (keyword (:slug row))]
      (if (contains? content key)
        (assoc values key (content key))
        values)))
  (post-update [this content opts] content)
  (pre-destroy [this content] content)
  (join-fields [this prefix opts] [])
  (join-conditions [this prefix opts] [])
  (build-where
    [this prefix opts]
    (field/field-where this prefix opts field/string-where))
  (natural-orderings [this prefix opts])
  (build-order [this prefix opts]
    (field/pure-order this prefix opts))
  (field-generator [this generators]
    (assoc generators (keyword (:slug row))
           (fn [] (util/rand-str (+ 141 (rand-int 5555))))))
  (fuse-field [this prefix archetype skein opts]
    (field/text-fusion this prefix archetype skein opts))
  (localized? [this] true)
  (propagate-order [this id orderings])
  (models-involved [this opts all] all)
  (field-from [this content opts]
    (adapter/text-value @config/db-adapter (content (keyword (:slug row)))))
  (render [this content opts]
    (update-in
     content [(keyword (:slug row))]
     #(adapter/text-value @config/db-adapter %)))
  (validate [this opts] (validation/for-type this opts string? "text object")))

(defn constructor
  [row]
  (TextField. row {}))
