# Caribou Core

A basis for creating dynamic data models who themselves live as data within a model that represents data models.

## Usage

The heart of Caribou Core is the concept of the Model.  Each Model
represents a conceptual type within your application.  This Model has a number of
Fields which govern what kind of things each instance of your Model consists of.  
These Fields range from numbers and text to addresses, images and even 
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

## License

Copyright © Instrument

Distributed under the MIT License
