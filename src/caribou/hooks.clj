(ns caribou.hooks
  (:require [caribou.config :as config]
            [caribou.logger :as log]))

;; HOOKS -------------------------------------------------------

(defn default-lifecycle-hooks
  "establish the set of functions which are called throughout the
  lifecycle of all rows for a given model (slug).  the possible hook
  points are:

    :before-create -- called for create only, before the record is made
    :after-create -- called for create only, now the record has an id
    :before-update -- called for update only, before any changes are made
    :after-update -- called for update only, now the changes have been committed
    :before-save -- called for create and update
    :after-save -- called for create and update
    :before-destroy -- only called on destruction, record has not yet been removed
    :after-destroy -- only called on destruction, now the db has no record of it"

  []
  {:before-create  {}
   :after-create   {}
   :before-update  {}
   :after-update   {}
   :before-save    {}
   :after-save     {}
   :before-destroy {}
   :after-destroy  {}})

(defn hooks-for-model
  [slug]
  (get (deref (config/draw :hooks :lifecycle)) (keyword slug)))

(defn make-lifecycle-hooks
  [slug]
  (if-not (hooks-for-model slug)
    (let [hooks (default-lifecycle-hooks)]
      (swap! (config/draw :hooks :lifecycle) assoc-in [(keyword slug)] hooks))))

(defn run-hook
  "run the hooks for the given model slug given by timing.  env
  contains any necessary additional information for the running of the
  hook"
  [slug timing env]
  (if-let [kind (hooks-for-model slug)]
    (let [hook (get kind (keyword timing))]
      (reduce
       (fn [env key]
         ((get hook key) env))
       env (keys hook)))
    env))

(defn add-hook
  "add a hook for the given model slug for the given timing.  each hook
  must have a unique id, or it overwrites the previous hook at that
  id."
  [slug timings id func]
  (let [timings (if (keyword? timings) [timings] timings)]
    (doseq [timing timings]
      (if-let [model-hooks (hooks-for-model slug)]
        (if-let [hook (get model-hooks (keyword timing))]
          (let [hook-name (keyword id)]
            (swap! (config/draw :hooks :lifecycle) assoc-in [(keyword slug) (keyword timing) hook-name] func))
          (throw (Exception. (format "No model lifecycle hook called %s" timing))))
        (throw (Exception. (format "No model called %s" slug)))))))

