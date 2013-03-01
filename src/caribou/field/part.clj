(ns caribou.field.part
  (:require [clojure.string :as string]
            [caribou.field :as field]
            [caribou.util :as util]
            [caribou.db :as db]
            [caribou.validation :as validation]
            [caribou.association :as assoc]))


(defn part-where
  [field prefix opts]
  (let [slug (-> field :row :slug)]
    (assoc/with-propagation :where opts slug
      (fn [down]
        (let [model (field/models (-> field :row :model_id))
              target (field/models (-> field :row :target_id))
              part (-> field :row :slug)
              part-id-slug (keyword (str part "_id"))
              part-id-field (-> model :fields part-id-slug)
              part-select (field/coalesce-locale model part-id-field prefix
                                                  (name part-id-slug) opts)
              id-field (-> target :fields :id)
              table-alias (str prefix "$" slug)
              field-select (field/coalesce-locale model id-field table-alias
                                                   "id" opts)
              subconditions (assoc/model-where-conditions target table-alias down)
              params [part-select field-select (:slug target) table-alias subconditions]]
          (util/clause "%1 in (select %2 from %3 %4 where %5)" params))))))

(defrecord PartField [row env]
  field/Field

  (table-additions [this field] [])
  (subfield-names [this field] [(str field "_id") (str field "_position")])

  (setup-field [this spec]
    (let [model-id (:model_id row)
          model (db/find-model model-id @field/models)
          target (db/find-model (:target_id row) @field/models)
          reciprocal-name (or (:reciprocal_name spec) (:name model))
          id-slug (str (:slug row) "_id")]
      (if (or (nil? (:link_id row)) (zero? (:link_id row)))
        (let [collection ((resolve 'caribou.model/create) :field
                           {:name reciprocal-name
                            :type "collection"
                            :model_id (:target_id row)
                            :target_id model-id
                            :link_id (:id row)})]
          (db/update :field ["id = ?" (util/convert-int (:id row))] {:link_id (:id collection)})))

      ((resolve 'caribou.model/update) :model model-id
       {:fields
        [{:name (util/titleize id-slug)
          :type "integer"
          :editable false
          :reference (:slug target)
          :dependent (:dependent spec)}
         {:name (util/titleize (str (:slug row) "_position"))
          :type "integer"
          :editable false}]} {:op :migration})))

  (rename-field [this old-slug new-slug])

  (cleanup-field [this]
    (let [model (@field/models (row :model_id))
          fields (:fields model)
          id-slug (keyword (str (:slug row) "_id"))
          position (keyword (str (:slug row) "_position"))]
      (db/drop-index (:slug model) id-slug)
      ((resolve 'caribou.model/destroy) :field (-> fields id-slug :row :id))
      ((resolve 'caribou.model/destroy) :field (-> fields position :row :id))
      (try
        (do ((resolve 'caribou.model/destroy) :field (-> env :link :id)))
        (catch Exception e (str e)))))

  (target-for [this] (@field/models (-> this :row :target_id)))

  (update-values [this content values] values)

  (post-update [this content opts] content)

  (pre-destroy [this content] content)

  (join-fields [this prefix opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (let [target (@field/models (:target_id row))]
          (assoc/model-select-fields target (str prefix "$" (:slug row))
                                     down)))))

  (join-conditions [this prefix opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (let [model (@field/models (:model_id row))
              target (@field/models (:target_id row))
              id-slug (keyword (str (:slug row) "_id"))
              id-field (-> model :fields id-slug)
              table-alias (str prefix "$" (:slug row))
              field-select (field/coalesce-locale model id-field prefix
                                                   (name id-slug) opts)
              downstream (assoc/model-join-conditions target table-alias down)
              params [(:slug target) table-alias field-select]]
          (concat
           [(util/clause "left outer join %1 %2 on (%3 = %2.id)" params)]
           downstream)))))

  (build-where
    [this prefix opts]
    (part-where this prefix opts))

  (natural-orderings [this prefix opts]
    (let [target (@field/models (:target_id row))
          downstream (assoc/model-natural-orderings target (str prefix "$" (:slug row)) opts)]
      downstream))

  (build-order [this prefix opts]
    (assoc/join-order this (@field/models (:target_id row)) prefix opts))

  (fuse-field [this prefix archetype skein opts]
    (assoc/part-fusion this (@field/models (-> this :row :target_id)) prefix archetype skein opts))

  (localized? [this] false)

  (models-involved [this opts all]
    (assoc/span-models-involved this opts all))

  (field-from [this content opts]
    (assoc/with-propagation :include opts (:slug row)
      (fn [down]
        (if-let [pointing (content (keyword (str (:slug row) "_id")))]
          (let [collector (db/choose (-> (field/target-for this) :slug) pointing)]
            (assoc/from (field/target-for this) collector down))))))

  (render [this content opts]
    (assoc/part-render this (@field/models (:target_id row)) content opts))

  (validate [this opts] (validation/for-assoc this opts)))

(defn constructor
  [row]
  (let [link (db/choose :field (row :link_id))]
    (PartField. row {:link link})))
                      