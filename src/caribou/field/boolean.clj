(ns caribou.field.boolean
  (:require [caribou.field-protocol :as field]
            [caribou.validation :as validation]))

(defrecord BooleanField [row env]
  field/Field
  (table-additions [this field] [[(keyword field) :boolean]])
  (subfield-names [this field] [])
  (setup-field [this spec] nil)
  (rename-field [this old-slug new-slug])
  (cleanup-field [this] nil)
  (target-for [this] nil)
  (update-values [this content values]
    (let [key (keyword (:slug row))]
      (if (contains? content key)
        (try
          (let [value (content key)
                tval (if (isa? (type value) String)
                       (Boolean/parseBoolean value)
                       value)]
            (assoc values key tval))
          (catch Exception e values))
        values)))
  (post-update [this content opts] content)
  (pre-destroy [this content] content)
  (join-fields [this prefix opts] [])
  (join-conditions [this prefix opts] [])
  (build-where
    [this prefix opts]
    (field/field-where this prefix opts field/pure-where))
  (natural-orderings [this prefix opts])
  (build-order [this prefix opts]
    (field/pure-order this prefix opts))
  (field-generator [this generators]
    (assoc generators (keyword (:slug row)) (fn [] (= 1 (rand-int 2)))))
  (fuse-field [this prefix archetype skein opts]
    (field/boolean-fusion this prefix archetype skein opts))
  (localized? [this] (not (:locked row)))
  (models-involved [this opts all] all)
  (field-from [this content opts] (content (keyword (:slug row))))
  (render [this content opts] content)
  (validate [this opts] (validation/for-type this opts
                                             (fn [x]
                                               (not (nil?
                                                     (#{true false 0 1}
                                                      x))))
                                             "boolean")))

(field/add-constructor :boolean (fn [row operations] (BooleanField. row {})))
                      