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

(defmacro with-config
  "Use our explicit configuration while logging something, with
  dynamic additions in a map. Explicitly using with-config on every
  logging call avoids some pervasive weirdness with configuration
  clobbering."
  [map & body]
  `(logconf/with-logging-config
     [:root (merge
             {:level (:log-level @defaults)
              ;; pattern is done this way because the java logging tools
              ;; don't understand where we are in clojure land
              :pattern ;(-> (:log-pattern @defaults)
                       ;    ;; location
                       ;    #(clojure.string/replace % #"%l" "%M : %F (%L)")
                       ;    ;; method (function?), cannot find this atm
                       ;                 ;#(clojure.string/replace % #"%M" "")
                       ;    (clojure.string/replace % #"%F" ~*file*)
                       ;    (clojure.string/replace % #"%L"
                       ;                            ~(:line (meta &form)))))
              (:log-pattern @defaults)
              :filter (:log-filter @defaults)}
             ~map)]
     ~@body))

(defmacro debug 
  "Log a debug message (with an optional prefix)"
  ([msg] `(with-config {} (logging/debug ~msg)))
  ([msg prefix] `(debug (str (name ~prefix) ": " ~msg))))

(defn info 
  "Log an info message (with an optional prefix)"
  ([msg] `(with-config {} (logging/info ~msg)))
  ([msg prefix] `(info (str (name ~prefix) ": " ~msg))))

(defn warn 
  "Log a warning message (with an optional prefix)"
  ([msg] `(with-config {} (logging/warn ~msg)))
  ([msg prefix] `(warn (str (name ~prefix) ": " ~msg))))

(defn error 
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
