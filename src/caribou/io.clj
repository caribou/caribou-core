(ns caribou.io
  (:require [clojure.java.io :as io]
            [caribou.config :as config]
            [caribou.db :as db]
            [caribou.model :as model]))

(defn compound-models
  ([models] (compound-models models {}))
  ([models opts]
     (reduce 
      (fn [export model]
        (let [key (keyword model)
              all (model/gather key (get opts key))]
          (assoc export key all)))
      {} models)))

(defn ensure-directory
  [target]
  (let [file (io/file target)
        parent (.getParentFile file)]
    (if-not (.exists parent)
      (.mkdirs parent))))

(defn export-map
  [map target]
  (ensure-directory target)
  (with-open [out (io/writer target)]
    (binding [*out* out]
      (pr map))))

(defn export-models
  ([models opts target]
     (export-models models opts target identity))
  ([models opts target pre]
     (ensure-directory target)
     (let [all (compound-models models opts)
           processed (pre all)]
       (export-map processed target))))

(def schema-models [:model :field :enumeration :locale :status])

(defn export-schema
  ([] (export-schema "export/schema.clj"))
  ([target]
     (let [built-in-models (map :id (model/gather :model {:where {:locked true}}))
           opts {:model {:where {:locked false}}
                 :field {:where {'or [{'not {:model-id built-in-models}} {'not {:target-id built-in-models}}]}}
                 :status {:where {'not {:slug ["draft" "published"]}}}}
           schema (compound-models schema-models opts)]
       (export-models schema-models opts target))))
       ;; (export-map schema target))))

(defn user-model-keys
  []
  (let [user-models (model/gather :model {:where {:locked false}})]
    (map (comp keyword :slug) user-models)))

(defn non-schema-keys
  []
  (let [all-models (map (comp keyword :slug) (model/gather :model))]
    (remove (set schema-models) all-models)))

(defn export-content
  ([] (export-content "export/content.clj"))
  ([target] 
     (let [models (non-schema-keys)
           opts {:site {:where {'not {:id 1}}}
                 :account {:where {'not {:id 1}}}
                 :role {:where {'not {:id 1}}}
                 :permission {:where {'not {:role-id 1}}}
                 :page {:where {'not {:id 1}}}}]
     (export-models models opts target))))
       
(defn generate-id-map
  [model-key items]
  (reduce 
   (fn [id-map item]
     (let [old-id (:id item)
           new-item (model/create model-key (dissoc item :id))]
       (assoc id-map old-id new-item)))
   {} items))

(defn sync-remote-ids
  [model-key id-map remote-key remote-ids]
  (update-in 
   id-map [model-key]
   (fn [model-map]
     (reduce
      (fn [model-map old-id]
        (update-in 
         model-map [old-id]
         (fn [item]
           (if-let [pointer (get item remote-key)]
             (let [new-id (get-in remote-ids [pointer :id])]
               (println "SYNCING REMOTE KEY" model-key remote-key (get item remote-key) new-id (str (keys remote-ids)))
               (assoc item 
                 remote-key (or new-id pointer)
                 :_synced true))
             item))))
      model-map (keys model-map)))))

(def remote-field-types 
  {"part" :*
   "tie" :*
   "asset" :asset
   "address" :location
   "enum" :enumeration})

(defn find-remote-associations
  [model-key model-id-map]
  (let [model (model/models model-key)
        fields (map :row (vals (:fields model)))
        remote (filter (comp remote-field-types :type) fields)]
    (map 
     (fn [field]
       (let [target-model (get remote-field-types (:type field))
             target-model (if (= target-model :*)
                            (keyword (:slug (get model-id-map (:target-id field))))
                            target-model)]
         {:model-key model-key
          :target-model target-model
          :remote-key (keyword (str (:slug field) "-id"))}))
     remote)))

(defn sync-remote-associations
  [id-map model-key model-id-map]
  (let [all-remote (find-remote-associations model-key model-id-map)]
    (reduce
     (fn [id-map remote]
       (sync-remote-ids 
        model-key id-map 
        (:remote-key remote)
        (get id-map (:target-model remote))))
     id-map all-remote)))

(defn update-field-target-ids
  [field-id-map model-id-map]
  (into 
   {}
   (map 
    (fn [[field-id new-field]]
      (if-let [old-target-id (:target-id new-field)]
        (let [new-target-id (or (:id (get model-id-map old-target-id)) old-target-id)]
          (model/update 
           :field field-id {:target-id new-target-id :slug (:slug new-field)})
          [field-id (assoc new-field :target-id new-target-id)])
        [field-id new-field]))
    field-id-map)))

(defn import-schema
  ([] (import-schema "export/schema.clj"))
  ([target]
     (with-open [in (java.io.PushbackReader. (io/reader target))]
       (let [all (read in)

             ;; save hooks for later and neutralize to isolate side 
             ;; effects while importing
             hooks (deref (config/draw :hooks :lifecycle))
             _ (swap! (config/draw :hooks :lifecycle) (constantly {}))

             ;; temporarily disable field foreign key constraint
             _ (db/drop-reference :field :model-id :model)

             ;; build map of old ids to new items for each model
             id-map (reduce 
                     (fn [id-map [model-key items]]
                       (let [model-map (generate-id-map model-key items)]
                         (assoc id-map model-key model-map)))
                     {} all)

             ;; update all field's target ids to point to the new ids for those models
             id-map (update-in 
                     id-map [:field] 
                     #(update-field-target-ids % (:model id-map)))

             ;; get all currently existing models by id
             models-by-id (reduce
                           (fn [models-by-id model]
                             (assoc models-by-id (:id model) model))
                           {} (model/gather :model))

             ;; old-model-id-map (:model id-map)

             ;; ;; merge in the current model id map
             ;; id-map (update-in id-map [:model] #(merge models-by-id %))

             ;; point all the old remote ids to the new ids for those same items
             id-map (reduce 
                     (fn [id-map model-key]
                       (sync-remote-associations id-map model-key models-by-id))
                     id-map (keys id-map))

             ;; perform db updates on all items who have updated remote ids
             _ (doseq [[model-key model-map] id-map]
                 (doseq [item (vals model-map)]
                   (if (:_synced item)
                     (model/update model-key (:id item) (dissoc item :_synced)))))]

         ;; now do all necessary hooks for :model, :field and :locale!
         (doseq [model (vals (:model id-map))] ;; old-model-id-map)]
           (db/create-table 
            (keyword (:slug model)) 
            [:id "SERIAL" "PRIMARY KEY"]))

         (doseq [field (vals (:field id-map))]
           (let [field-field (model/make-field field)
                 model-slug (get-in models-by-id [(:model-id field) :slug])]
             (model/add-db-columns-for-field field-field model-slug (:slug field))
             (model/prepare-db-field field-field model-slug (:slug field) field)))

         (doseq [model (vals (:model id-map))] ;; old-model-id-map)]
           (db/create-index (:slug model) "id")
           (db/create-index (:slug model) "uuid"))

         (doseq [locale (vals (:locale id-map))]
           (model/add-locale locale))

         ;; reenable field foreign key constraint
         (db/add-reference :field :model-id :model :destroy)

         ;; finally, invoke new models!
         (model/invoke-models)))))


(defn import-content
  ([] (import-schema "export/content.clj"))
  ([target]
     (with-open [in (java.io.PushbackReader. (io/reader target))]
       (let [all (read in)

             ;; save hooks for later and neutralize to isolate side 
             ;; effects while importing
             hooks (deref (config/draw :hooks :lifecycle))
             _ (swap! (config/draw :hooks :lifecycle) (constantly {}))

             ;; build map of old ids to new items for each model
             id-map (reduce 
                     (fn [id-map [model-key items]]
                       (let [model-map (generate-id-map model-key items)]
                         (assoc id-map model-key model-map)))
                     {} all)

             ;; get all currently existing models by id
             models-by-id (reduce
                           (fn [models-by-id model]
                             (assoc models-by-id (:id model) model))
                           {} (model/gather :model))

             ;; point all the old remote ids to the new ids for those same items
             id-map (reduce 
                     (fn [id-map model-key]
                       (sync-remote-associations id-map model-key models-by-id))
                     id-map (keys id-map))]

         ;; perform db updates on all items who have updated remote ids
         (doseq [[model-key model-map] id-map]
           (doseq [item (vals model-map)]
             (if (:_synced item)
               (model/update model-key (:id item) (dissoc item :_synced)))))))))
