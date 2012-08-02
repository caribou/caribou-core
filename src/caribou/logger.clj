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

(defn unwrap-conf
  "this avoids some issues with macro-expansion in with-config"
  [file line]
  (fn [pattern]
    (clojure.string/replace
     (clojure.string/replace pattern; two passes needed here
                             "%l" "%F : %M (%L)") ; since this one is recursive
     #"%M|%F|%L"
     {"%M" "" ; dunno how to find method / function yet, %M just gives us "write_BANG_"
      "%F" ((fnil identity "") file) ; file will be a string or nil
      "%L" (str line)}))) ; line will be a number or nil

(defmacro with-config
  "Use our explicit configuration while logging something, with
  dynamic additions in a map. Explicitly using with-config on every
  logging call avoids some pervasive weirdness with configuration
  clobbering."
  [map & body]
  `(let [customized# (merge ~map @defaults)
         base# (merge {:level (:log-level customized#)
                       :pattern (:log-pattern customized#)
                       :filter (:log-filter customized#)}
                      ;; merging this again because it may have keys not
                      ;; caught above
                      ~map)
         total# (update-in base# [:pattern]
                           (unwrap-conf (:file ~map) (:line ~map)))]
     (logconf/with-logging-config
       [:root total#]
       ~@body)))

(defmacro debug
  "Log a debug message (with an optional prefix)"
  ([msg] `(with-config {:line ~(:line (meta &form)) :file ~*file*}
            (logging/debug ~msg)))
  ([msg prefix] `(debug (str (name ~prefix) ": " ~msg))))

(defmacro info
  "Log an info message (with an optional prefix)"
  ([msg] `(with-config {:line ~(:line (meta &form)) :file ~*file*}
            (logging/info ~msg)))
  ([msg prefix] `(info (str (name ~prefix) ": " ~msg))))

(defmacro warn
  "Log a warning message (with an optional prefix)"
  ([msg] `(with-config {:line ~(:line (meta &form)) :file ~*file*}
            (logging/warn ~msg)))
  ([msg prefix] `(warn (str (name ~prefix) ": " ~msg))))

(defmacro error 
  "Log an error message (with an optional prefix)"
  ([msg] `(with-config {:line ~(:line (meta &form)) :file ~*file*}
            (logging/error ~msg)))
  ([msg prefix] `(error (str (name ~prefix) ": " ~msg))))

(defmacro spy
  "Spy the value of an expression (with an optional prefix)"
  ([form] `(logconf/with-logging-config {:line ~(:line (meta &form)) :file ~*file*}
             (logging/spy '~form)))
  ([form prefix] `(logconf/with-logging-config
                    {:pattern 
                     (str ~prefix " "
                          (:log-pattern @config/app))}
                    (logging/spy '~form))))
