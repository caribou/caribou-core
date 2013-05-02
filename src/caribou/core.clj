(ns caribou.core
  (:require [caribou.config :as config]
            [caribou.model :as model]
            [caribou.logger :as log]))

(defn init
  [init-config]
  (let [full-config (config/process-config init-config)]
    (config/with-config full-config
      (model/db
       (fn []
         (model/invoke-models)
         (log/init (config/draw :logging :loggers))
         (config/draw))))))

(defmacro with-caribou
  [config & body]
  `(model/with-models ~config
     ~@body))
