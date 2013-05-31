(ns caribou.repl
  (:require [clojure.tools.nrepl.server :as nrepl]
            [caribou.config :as config]
            [caribou.core :as caribou]))

(defn caribou-repl
  [config]
  (let [handler (nrepl/default-handler)
        config (caribou/init config)]
    (fn [{:keys [op transport] :as msg}]
      (caribou/with-caribou config
        (handler msg)))))

(defn repl-init
  []
  (if-let [port (config/draw :nrepl :port)]
    (let [server (nrepl/start-server :port port :handler (caribou-repl (config/draw)))]
      (reset! (config/draw :nrepl :server) server))))