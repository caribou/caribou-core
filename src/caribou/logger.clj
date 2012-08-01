(ns caribou.logger
  (:require [clj-logging-config.log4j :as logconf]
            [clojure.tools.logging :as logging]))

;; initialize clj-logging-config
(def defaults (ref
               {:log-pattern "%p %m (%x) %n\n"
                :log-level :debug
                :log-filter (constantly true)
                :debug true}))

(defn init
  []
  (logconf/set-logger! :pattern (:log-pattern @defaults)
                       :level (:log-level @defaults)
                       :filter (:log-filter @defaults)))

(defn replace-strings
  "helper to simplify capture-pattern-context"
  [str rep]
  (clojure.string/replace str (rep 0) (rep 1)))

(defmacro capture-pattern-context
  [pattern]
  `(reduce
    replace-strings
    pattern
    [;; location
     [#"%l" "%M : %F (%L)"]
     ;; method (function?), cannot find this atm
     [#"%M" ""]
     ;; file name
     [#"%F" ~*file*]
     ;; line number
     [#"%L" (str ~(:line (meta &form)))]]))

(defmacro with-config
  "Use our explicit configuration while logging something, with
  dynamic additions in a map. Explicitly using with-config on every
  logging call avoids some pervasive weirdness with configuration
  clobbering."
  [map & body]
  `(let [#config (merge
                  {:level (:log-level @defaults)
                   :pattern (:log-pattern @defaults)
                   :filter (:log-filter @defaults)}
                  ~map)
         #config (update-in #config [:pattern]
                            capture-pattern-context)]
     (logconf/with-logging-config
       [:root #config]
       ~@body)))

(defmacro debug
  "Log a debug message (with an optional prefix)"
  ([msg] `(with-config {} (logging/debug ~msg)))
  ([msg prefix] `(debug (str (name ~prefix) ": " ~msg))))

(defmacro info 
  "Log an info message (with an optional prefix)"
  ([msg] `(with-config {} (logging/info ~msg)))
  ([msg prefix] `(info (str (name ~prefix) ": " ~msg))))

(defmacro warn 
  "Log a warning message (with an optional prefix)"
  ([msg] `(with-config {} (logging/warn ~msg)))
  ([msg prefix] `(warn (str (name ~prefix) ": " ~msg))))

(defmacro error 
  "Log an error message (with an optional prefix)"
  ([msg] `(with-config {} (logging/error ~msg)))
  ([msg prefix] `(error (str (name ~prefix) ": " ~msg))))

(defmacro spy
  "Spy the value of an expression (with an optional prefix)"
  ([form] `(logconf/with-logging-config {} (logging/spy '~form)))
  ([form prefix] `(logconf/with-logging-config
                    {:pattern 
                     (str prefix " "
                          (:log-pattern @config/app))}
                    (logging/spy '~form))))
