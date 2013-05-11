(ns caribou.repl
  (:require [clojure.tools.nrepl.server :as nrepl]
            [caribou.config :as config]))

(defn repl-init
  []
  (if-let [port (config/draw :nrepl :port)]
    (let [server (nrepl/start-server :port port)]
      (reset! (config/draw :nrepl :server) server))))

