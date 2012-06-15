# Caribou Core

A basis for creating dynamic data models who themselves live as data within a model that represents data models.

## Usage

The heart of Caribou is the concept of the Model.  Each model
represents a conceptual type within your application.  This model has a number of
Fields which govern what kind of things each instance of your model consists of.  
These fields range from numbers and text to addresses, images and even 
associations with other models.  

To begin, require and initialize Caribou:

```clj
(ns tundra.core
  (:require [caribou.model :as model]))

(model/init)
```

Then, let's pull some models up and print their names (showing effectively what 
the names are of all the built in models):

```clj
(def models (model/db #(model/gather :model {})))
(println (map :name models))
```

To create a new model, use the model/create function.  Then you can create instances
of your new model in exactly the same way (since everything is a model):

```clj
(model/db 
  (fn []
    (model/create :model {:name "Tree" :fields [{:name "Limbs" :type :integer} {:name "Species" :type :string}]})
    (model/create :tree {:limbs 3 :species "Acer circinatum"})
    (model/create :tree {:limbs 8 :species "Liquidambar styraciflua"})
    (println (map :species (model/gather :tree {:order {:limbs :desc}})))))
```

The most powerful act you can perform on a model is to link it, or associate it
to another model.  Links are just fields, with the added effect that they have a reciprocal relationship going in the other direction.

```clj
(model/db
  (fn []
    (let [tree-id (-> @model/models :tree :id) ;; there is a Ref containing all the models in memory.
          bird-model (model/create :model {:name "Bird" 
                                           :fields [{:name "Species" :type :string} 
                                                    {:name "Portrait" :type :asset}
                                                    {:name "Nests" :type :link :target_id tree-id 
                                                     :reciprocal_name "Inhabitors"}]})
          bird (model/create :bird {:species "Monticola gularis"})
          tree (model/pick :tree {:where {:limbs 3}})]
        (model/update :bird (:id bird) {:nests [tree]}))
    (println (model/gather :tree {:include {:inhabitors {}}}))))
```

## License

Copyright © Instrument

Distributed under the MIT License
