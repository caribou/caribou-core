(ns caribou.field.password
  (:require [caribou.field :as field]
            [caribou.util :as util]
            [caribou.auth :as auth]
            [caribou.validation :as validation]))

(defn crypted-slug
  [slug]
  (keyword (str "crypted-" (name slug))))

(defn password-setup-field
  [field spec]
  (let [slug (-> field :row :slug)
        crypted (crypted-slug slug)
        model-id (-> field :row :model-id)]
    ((resolve 'caribou.model/update) :model model-id
     {:fields [{:name (util/titleize crypted)
                :type "string"
                :editable false}]} {:op :migration})))

(defn password-cleanup-field
  [field]
  (let [model (field/models (-> field :row :model-id))
        slug (-> field :row :slug)
        crypted (crypted-slug slug)]
    ((resolve 'caribou.model/destroy) :field (-> model :fields crypted :row :id))))

(defn password-update-values
  [field content values]
  (let [slug (-> field :row :slug)
        key (keyword slug)
        crypted (keyword (str "crypted-" slug))]
    (if (contains? content key)
      (assoc values crypted (auth/hash-password (get content key)))
      values)))

(defrecord PasswordField [row env]
  field/Field
  (table-additions [this field] [])
  (subfield-names [this field] [])
  (setup-field [this spec]
    (password-setup-field this spec))
  (rename-model [this old-slug new-slug])
  (rename-field [this old-slug new-slug])
  (cleanup-field [this]
    (password-cleanup-field this))
  (target-for [this] nil)
  (update-values [this content values]
    (password-update-values this content values))
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
  (propagate-order [this id orderings])
  (models-involved [this opts all] all)
  (field-from [this content opts]
    (content (keyword (:slug row))))
  (render [this content opts] content)
  (validate [this opts] (validation/for-type this opts string? "password")))

(defn constructor
  [row]
  (PasswordField. row {}))

