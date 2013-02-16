(ns caribou.field.decimal
  (:require [caribou.field-protocol :as field]
            [caribou.validation :as validation]))

(defrecord DecimalField [row env]
  field/Field
  (table-additions [this field] [[(keyword field) "decimal(20,10)"]])
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
                       (BigDecimal. value)
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
    (assoc generators (keyword (:slug row)) (fn [] (rand))))
  (fuse-field [this prefix archetype skein opts]
    (field/pure-fusion this prefix archetype skein opts))
  (localized? [this] true)
  (models-involved [this opts all] all)
  (field-from [this content opts] (content (keyword (:slug row))))
  (render [this content opts]
    (update-in content [(keyword (:slug row))] str))
  (validate [this opts] (validation/for-type this opts number? "decimal")))
  
(field/add-constructor :decimal (fn [row] (DecimalField. row {})))