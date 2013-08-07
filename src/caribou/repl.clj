(ns caribou.repl
  (:require [clojure.tools.nrepl.server :as nrepl]
            [complete.core :as complete]
            [caribou.logger :as log]
            [caribou.config :as config]
            [caribou.core :as caribou]))

(defn caribou-repl
  [config]
  (let [handler (nrepl/default-handler)
        config (caribou/init config)]
    (fn [{:keys [op transport] :as msg}]
      (caribou/with-caribou config
        (let [caribou-msg (update-in msg [:code] #(str "(caribou.core/with-caribou caribou.config/config " % ")"))]
          (handler caribou-msg))))))

(defn repl-init
  []
  (if-let [port (config/draw :nrepl :port)]
    (let [server (nrepl/start-server :port port :handler (caribou-repl (config/draw)))]
      (log/info (str "Starting nrepl server on port :" port) :nrepl)
      (reset! (config/draw :nrepl :server) server))))
