(ns caribou.field.constructors
  (:require [caribou.field.id :as id-field]
            [caribou.field.integer :as integer-field]
            [caribou.field.decimal :as decimal-field]
            [caribou.field.string :as string-field]
            [caribou.field.password :as password-field]
            [caribou.field.text :as text-field]
            [caribou.field.slug :as slug-field]
            [caribou.field.urlslug :as urlslug-field]
            [caribou.field.boolean :as boolean-field]
            [caribou.field.asset :as asset-field]
            [caribou.field.address :as address-field]
            [caribou.field.collection :as collection-field]
            [caribou.field.part :as part-field]
            [caribou.field.tie :as tie-field]
            [caribou.field.link :as link-field]
            [caribou.field.timestamp :as timestamp-field]))


(def base-constructors
  {:id id-field/constructor
   :integer integer-field/constructor
   :decimal decimal-field/constructor
   :string string-field/constructor
   :password password-field/constructor
   :text text-field/constructor
   :slug slug-field/constructor
   :urlslug urlslug-field/constructor
   :boolean boolean-field/constructor
   :asset asset-field/constructor
   :address address-field/constructor
   :collection collection-field/constructor
   :part part-field/constructor
   :tie tie-field/constructor
   :link link-field/constructor
   :timestamp timestamp-field/constructor})
