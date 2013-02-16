(ns caribou.field.password
  (:require [caribou.field-protocol :as field]
            [caribou.util :as util]
            [caribou.auth :as auth]
            [caribou.validation :as validation]))

(defrecord PasswordField [row env]
  field/Field
  (table-additions [this field] [[(keyword field) "varchar(255)"]])
  (subfield-names [this field] [])
  (setup-field [this spec] nil)
  (rename-field [this old-slug new-slug])
  (cleanup-field [this] nil)
  (target-for [this] nil)
  (update-values [this content values]
    (let [key (keyword (:slug row))]
      (if (contains? content key)
        (assoc values key (auth/hash-password (content key)))
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
           (fn [] (util/rand-str 13))))
  (fuse-field [this prefix archetype skein opts]
    (field/pure-fusion this prefix archetype skein opts))
  (localized? [this] true)
  (models-involved [this opts all] all)
  (field-from [this content opts]
    (content (keyword (:slug row))))
  (render [this content opts] content)
  (validate [this opts] (validation/for-type this opts string? "password")))

(field/add-constructor :password (fn [row] (PasswordField. row {})))


