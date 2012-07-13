(ns caribou.logger
  (:require [clj-logging-config.log4j :as logconf]
            [clojure.tools.logging :as logging]))

;; initialize clj-logging-config
(logconf/set-logger!)

(def defaults (ref
               {:log-layout
                (org.apache.log4j.PatternLayout. "%p %m (%x) %n\n")
                :log-level :debug
                :log-filter (constantly true)
                :debug true}))

(defmacro with-config
  "Use our explicit configuration while logging something, with
  dynamic additions in a map. Explicitly using with-config on every
  logging call avoids some pervasive weirdness with configuration
  clobbering."
  [map & body]
  `(logconf/with-logging-config
     [:root (merge
             {:level (:log-level @defaults)
              :layout (:log-layout @defaults)
              :filter (:log-filter @defaults)}
             ~map)]
     ~@body))

(defn debug 
  "Log a debug message (with an optional prefix)"
  ([msg] (with-config {} (logging/debug msg)))
  ([msg prefix] (debug (str (name prefix) ": " msg))))

(defn info 
  "Log an info message (with an optional prefix)"
  ([msg] (with-config {} (logging/info msg)))
  ([msg prefix] (info (str (name prefix) ": " msg))))

(defn warn 
  "Log a warning message (with an optional prefix)"
  ([msg] (with-config {} (logging/warn msg)))
  ([msg prefix] (warn (str (name prefix) ": " msg))))

(defn error 
  "Log an error message (with an optional prefix)"
  ([msg] (with-config {} (logging/error msg)))
  ([msg prefix] (error (str (name prefix) ": " msg))))

(defmacro spy
  "Spy the value of an expression (with an optional prefix)"
  ([form] `(logconf/with-logging-config {} (logging/spy '~form)))
  ([form prefix] `(logconf/with-logging-config
                    {:log-layout (org.apache.log4j.PatternLayout.
                                  (str prefix " "
                                       (.getConversionPattern
                                        (:log-layout @config/app))))}
                    (logging/spy '~form))))
