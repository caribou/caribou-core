(ns ^{:skip-wiki true}
  caribou.migrations.bootstrap
  (:require [caribou.db :as db]
            [caribou.util :as util]
            [caribou.logger :as log]
            [caribou.auth :as auth]
            [caribou.model :as model]))

(defn create-migration-table []
  (db/create-table
   :migration
   [:id "SERIAL" "PRIMARY KEY"]
   [:name "varchar(55)" "NOT NULL" "UNIQUE"]
   [:run-at "timestamp" "NOT NULL" "DEFAULT current_timestamp"]))

(defn create-models-table []
  (db/create-table
   :model
   [:id "SERIAL" "PRIMARY KEY"]
   [:name "varchar(55)" "NOT NULL" "UNIQUE"]
   [:slug "varchar(55)" "NOT NULL" "UNIQUE"]
   [:description :text]
   [:position :integer "DEFAULT 0"]
   [:nested :boolean "DEFAULT false"]
   [:locked :boolean "DEFAULT false"]
   [:join-model :boolean "DEFAULT false"]
   [:abstract :boolean "DEFAULT false"]
   [:localized :boolean "DEFAULT false"]
   [:searchable :boolean "DEFAULT false"]
   [:ancestor-id :integer "DEFAULT NULL"]
   [:created-at "timestamp" "NOT NULL" "DEFAULT current_timestamp"]
   [:updated-at "timestamp" "NOT NULL"])) ;; "DEFAULT current_timestamp"]))

(defn create-field-table []
  (db/create-table
   :field
   [:id "SERIAL" "PRIMARY KEY"]
   [:name "varchar(55)" "NOT NULL"]
   [:slug "varchar(55)" "NOT NULL"]
   [:type "varchar(256)" "NOT NULL"]
   [:default-value "varchar(256)"]
   [:link-id :integer "DEFAULT NULL"]
   [:model-id :integer "NOT NULL" "REFERENCES model ON DELETE CASCADE"]
   [:model-position :integer "DEFAULT 0"]
   [:target-id :integer "DEFAULT NULL"]
   [:target-type "varchar(55)" "DEFAULT NULL"]
   [:description :text]
   [:position :integer "DEFAULT 0"]
   [:required :boolean "DEFAULT false"]
   [:disjoint :boolean "DEFAULT false"]
   [:singular :boolean "DEFAULT false"]
   [:locked :boolean "DEFAULT false"]
   [:immutable :boolean "DEFAULT false"]
   [:editable :boolean "DEFAULT true"]
   [:searchable :boolean "DEFAULT false"]
   [:dependent :boolean "DEFAULT false"]
   [:created-at "timestamp" "NOT NULL" "DEFAULT current_timestamp"]
   [:updated-at "timestamp" "NOT NULL"])) ;; "DEFAULT current_timestamp"]))

(defn create-model-model []
  (db/insert
   :model
   {:name "Model"
    :slug "model"
    :description "base model for models"
    :position 1
    :updated-at (model/current-timestamp)
    :locked true}))

(defn create-field-model []
  (db/insert
   :model
   {:name "Field"
    :slug "field"
    :description "a model that specifies what fields a model has"
    :position 2
    :updated-at (model/current-timestamp)
    :locked true}))

(defn create-model-fields []
  (let [model-id ((first (util/query "select id from model where slug = 'model'")) :id)]
    (db/insert
     :field
     {:name "Id"
      :slug "id"
      :type "integer"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (let [name-field (db/insert
                      :field
                      {:name "Name"
                       :slug "name"
                       :type "string"
                       :locked true
                       :updated-at (model/current-timestamp)
                       :model-id model-id})]
      (db/insert
       :field
       {:name "Slug"
        :slug "slug"
        :type "slug"
        :locked true
        :editable false
        :updated-at (model/current-timestamp)
        :link-id (name-field :id)
        :model-id model-id}))
    (db/insert
     :field
     {:name "Description"
      :slug "description"
      :type "text"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Position"
      :slug "position"
      :type "position"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Nested"
      :slug "nested"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Join Model"
      :slug "join-model"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Fields"
      :slug "fields"
      :type "collection"
      :dependent true
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Locked"
      :slug "locked"
      :type "boolean"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Abstract"
      :slug "abstract"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Localized"
      :slug "localized"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Searchable"
      :slug "searchable"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Ancestor Id"
      :slug "ancestor-id"
      :type "integer"
      :locked true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Created At"
      :slug "created-at"
      :type "timestamp"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Updated At"
      :slug "updated-at"
      :type "timestamp"
      :locked true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})))

(defn create-field-fields []
  (let [model-id ((first (util/query "select id from model where slug = 'field'")) :id)]
    (db/insert
     :field
     {:name "Id"
      :slug "id"
      :type "integer"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Link"
      :slug "link"
      :type "tie"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Link Id"
      :slug "link-id"
      :type "integer"
      :locked true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (let [name-field (db/insert
                      :field
                      {:name "Name"
                       :slug "name"
                       :type "string"
                       :locked true
                       :updated-at (model/current-timestamp)
                       :model-id model-id})]
      (db/insert
       :field
       {:name "Slug"
        :slug "slug"
        :type "slug"
        :locked true
        :editable false
        :updated-at (model/current-timestamp)
        :link-id (name-field :id)
        :model-id model-id}))
    (db/insert
     :field
     {:name "Type"
      :slug "type"
      :type "string"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Default Value"
      :slug "default-value"
      :type "string"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Description"
      :slug "description"
      :type "text"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Position"
      :slug "position"
      :type "position"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Model Id"
      :slug "model-id"
      :type "integer"
      :locked true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Target Id"
      :slug "target-id"
      :type "integer"
      :locked true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Model Position"
      :slug "model-position"
      :type "position"
      :locked true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Model"
      :slug "model"
      :type "part"
      :locked true
      :dependent true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Required"
      :slug "required"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Disjoint"
      :slug "disjoint"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Singular"
      :slug "singular"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Editable"
      :slug "editable"
      :type "boolean"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Immutable"
      :slug "immutable"
      :type "boolean"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Locked"
      :slug "locked"
      :type "boolean"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Searchable"
      :slug "searchable"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Dependent"
      :slug "dependent"
      :type "boolean"
      :locked true
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Created At"
      :slug "created-at"
      :type "timestamp"
      :locked true
      :immutable true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})
    (db/insert
     :field
     {:name "Updated At"
      :slug "updated-at"
      :type "timestamp"
      :locked true
      :editable false
      :updated-at (model/current-timestamp)
      :model-id model-id})))

(defn forge-link []
  (let [model (first (db/fetch :model "slug = '%1'" "model"))
        field (first (db/fetch :model "slug = '%1'" "field"))
        collection (first (db/fetch :field "slug = '%1' and model_id = %2" "fields" (model :id)))
        part (first (db/fetch :field "slug = '%1' and model_id = %2" "model" (field :id)))]
    (db/update :field ["id = ?" (part :id)] {:link-id (collection :id) :target-id (model :id)})
    (db/update :field ["id = ?" (collection :id)] {:link-id (part :id) :target-id (field :id)})))

(defn lock [fields]
  (map #(assoc % :locked true) fields))

(def page {:name "Page"
           :description "center of all elements for a single request"
           :position 3
           :locked true
           :nested true
           :fields (lock [{:name "Name" :type "string" :required true :disjoint true}
                          {:name "Slug" :type "slug" :link-slug "name"}
                          {:name "Path" :type "string"}
                          {:name "Controller" :type "string" :required true}
                          {:name "Action" :type "string" :required true}
                          {:name "Method" :type "string"}
                          {:name "Template" :type "string"}])})

(def account {:name "Account"
              :description "representation of a person with a role and privileges"
              :position 4
              :locked true
              :fields (lock [{:name "First Name" :type "string"}
                             {:name "Last Name" :type "string"}
                             {:name "Handle" :type "string"}
                             {:name "Email" :type "string"}
                             {:name "Crypted Password" :type "string"}])})

(def view {:name "View"
           :description "a composition of content facets"
           :position 5
           :locked true
           :fields (lock [{:name "Name" :type "string"}
                          {:name "Description" :type "text"}])})

(def locale {:name "Locale"
             :description "a collection of content for a particular geographical audience"
             :position 6
             :locked true
             :fields (lock [{:name "Language" :type "string"}
                            {:name "Region" :type "string"}
                            {:name "Code" :type "string"}
                            {:name "Description" :type "text"}])})

(def asset {:name "Asset"
            :description "a reference to some system resource"
            :position 7
            :locked true
            :fields (lock [{:name "Filename" :type "string"}
                           {:name "Url" :type "string"}
                           {:name "Content Type" :type "string"}
                           {:name "Size" :type "integer"}
                           {:name "Parent Id" :type "integer"}
                           {:name "Description" :type "text"}])})

(def site {:name "Site"
           :description "maps to a particular set of pages"
           :position 8
           :locked true
           :fields (lock [{:name "Name" :type "string" :required true}
                          {:name "Slug" :type "slug" :link-slug "name"}
                          {:name "Asset" :type "asset"}
                          {:name "Description" :type "text"}])})

(def domain {:name "Domain"
             :description "each site may have several domain names that direct to its page set"
             :position 9
             :locked true
             :fields (lock [{:name "Name" :type "string"}
                            {:name "Description" :type "text"}])})

(def location {:name "Location"
               :description "a location somewhere on the planet"
               :position 10
               :locked true
               :fields (lock [{:name "Address" :type "string"}
                              {:name "Address Two" :type "string"}
                              {:name "City" :type "string"}
                              {:name "Postal Code" :type "string"}
                              {:name "State" :type "string"}
                              {:name "Country" :type "string"}
                              {:name "Lat" :type "decimal"}
                              {:name "Lng" :type "decimal"}])})

(def i18n {:name "i18n"
           :description "strings for internationalization"
           :position 11
           :locked true
           :fields (lock [{:name "Resource Key" :type "string"}
                          {:name "Value" :type "text"}])})

(def status {:name "Status"
             :description "all of the possible states a model can be in"
             :position 12
             :locked true
             :fields (lock [{:name "Name" :type "string" :model-position -1 :position -1} ; position hack
                            {:name "Slug" :type "slug" :link-slug "name"}
                            {:name "Description" :type "text"}])})

(defn create-default-status []
  (db/insert :status {:id 1
                      :name "Draft"
                      :slug "draft"
                      :description "Draft status is not publicly visible"
                      :locked true
                      :updated-at (model/current-timestamp)
                     })
  (db/insert :status {:id 2
                      :name "Published"
                      :slug "published"
                      :description "Published means visible to the public"
                      :locked true
                      :updated-at (model/current-timestamp)
                     }))

(defn create-default-user []
  (model/create :account {:email "caribou"
                          :first-name "Caribou"
                          :last-name "User"
                          :crypted-password (auth/hash-password "caribou")}))

(def incubating
  [page account view locale asset site domain location i18n status])

(defn spawn-models []
  (model/invoke-models)
  (doseq [spawn incubating]
    (log/debug (str "spawn models: " spawn))
    (model/create :model spawn)))

(defn build-links []
  (model/invoke-models)
  (model/update
   :model (model/models :site :id)
   {:fields
    [{:name "Domains"
      :type "collection"
      :dependent true
      :target-id (model/models :domain :id)}
     {:name "Pages"
      :type "collection"
      :target-id (model/models :page :id)}]}
   {:op :migration})
  (doseq [m (model/gather :model)] (model/add-status-to-model m)))

(defn drop-tables
  "Drops all the tables - useful during testing and/or building migrations"
  []
  (model/invoke-models)
  (doseq [spawn incubating]
    (db/drop-table (:name spawn)))
  (db/drop-table :field)
  (db/drop-table :model)
  (db/drop-table :migration))

(defn migrate
  []
  (create-migration-table)
  (create-models-table)
  (create-field-table)
  (create-model-model)
  (create-model-fields)
  (create-field-model)
  (create-field-fields)
  (forge-link)
  (spawn-models)
  (create-default-status)
  (create-default-user)
  (build-links))

(defn rollback
  []
  (drop-tables))
