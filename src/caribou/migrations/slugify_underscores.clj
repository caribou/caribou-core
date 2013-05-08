(ns caribou.migrations.slugify-underscores
  (:require [caribou.util :as util]
            [caribou.db :as db]))

(defn migrate
  []
  (let [slug-fields (db/fetch :field "type = 'slug'")]
    (doseq [slug-field slug-fields]
      (let [model (db/choose :model (:model-id slug-field))
            model-slug (:slug model)
            field-slug (-> slug-field :slug util/url-slugify keyword)]
        (doseq [content (db/fetch model-slug "1 = 1")]
          (let [underscored (get content field-slug)
                _ (println "FIELD SLUG:" field-slug underscored)
                new-slug (util/url-slugify underscored)]
            (db/update model-slug ["id = ?" (:id content)] {field-slug new-slug})))))))

(defn rollback
  [])