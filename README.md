# Caribou Core

A basis for creating dynamic data models who themselves live as data within a
model that represents data models.  In this way it is a cyclic self-creating
ouroboros of code/data.

## Usage

The heart of Caribou is the concept of the Model.  Each model represents a
conceptual type within your application.  This model has a number of Fields
which govern what kind of things each instance of your model consists of.  These
fields range from numbers and text to addresses, images and even associations
with other models.

First, you need to prepare a database:

    lein caribou migrate resources/config/development.clj

To begin, require and initialize Caribou:

```clj
(require '[caribou.model :as model] '[caribou.core :as caribou])
(def config (caribou/init-from-environment))
```

Then, let's pull some models up and print their names (showing effectively what 
the names are of all the built in models):

```clj
(def models
  (caribou/with-caribou config
    (model/gather :model)))

(println (map :name models))
```

To create a new model, use the model/create function.  Then you can create instances
of your new model in exactly the same way (since everything is a model):

```clj
(caribou/with-caribou config
  (model/create :model {:name "Tree" :fields [{:name "Limbs" :type "integer"} {:name "Species" :type "string"}]})
  (model/create :tree {:limbs 3 :species "Acer circinatum"})
  (model/create :tree {:limbs 8 :species "Liquidambar styraciflua"})
  (println (map :species (model/gather :tree {:order {:limbs :desc}}))))
```

The most powerful act you can perform on a model is to link it, or associate it
to another model.  Links are just fields, with the added effect that they have a
reciprocal relationship going in the other direction.

```clj
(caribou/with-caribou config
  (let [tree-id (model/models :tree :id)
        bird-model (model/create :model {:name "Bird" 
                                         :fields [{:name "Species" :type "string"} 
                                                  {:name "Portrait" :type "asset"}
                                                  {:name "Nests" :type "link" 
                                                   :target-id tree-id 
                                                   :reciprocal-name "Inhabitors"}]})
        bird (model/create :bird {:species "Monticola gularis"})
        tree (model/pick :tree {:where {:limbs 3}})]
     (model/update :bird (:id bird) {:nests [tree]}))
  (println (model/gather :tree {:include {:inhabitors {}}}))) ;; Acer circinatum will now contain an inhabitor
```

For further documentation go [here](http://caribou.github.io/caribou/docs/outline.html)!

## License

Copyright © Ryan Spangler

Distributed under the MIT License
