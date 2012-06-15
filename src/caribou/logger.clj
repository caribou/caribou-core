(ns caribou.logger
  (:require [clojure.tools.logging :as logging]))

(defn debug 
  "Log a debug message (with an optional prefix)"
  ([msg] (logging/debug msg))
  ([msg prefix] (debug (str (name prefix) ": " msg))))

(defn info 
  "Log an info message (with an optional prefix)"
  ([msg] (logging/info msg))
  ([msg prefix] (info (str (name prefix) ": " msg))))

(defn warn 
  "Log a warning message (with an optional prefix)"
  ([msg] (logging/warn msg))
  ([msg prefix] (warn (str (name prefix) ": " msg))))

(defn error 
  "Log an error message (with an optional prefix)"
  ([msg] (logging/error msg))
  ([msg prefix] (error (str (name prefix) ": " msg))))
