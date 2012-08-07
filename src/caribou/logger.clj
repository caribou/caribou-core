(ns caribou.logger
  (:require [clj-logging-config.log4j :as logconf]
            [clojure.tools.logging :as logging]))

;; initialize clj-logging-config
(def defaults (ref
               {:log-pattern "%-5p %u [%t]: %m %F %L%n"
                :log-level :debug
                :log-filter (constantly true)
                :debug true}))

(defn init
  []
  (logconf/set-logger! ;:pattern (:log-pattern @defaults)
                       :level (:log-level @defaults)
                       :filter (:log-filter @defaults)))

(defn unwrap-conf
  "this avoids some issues with macro-expansion in with-config"
  [file line user]
  (fn [pattern]
    (clojure.string/replace
     (clojure.string/replace pattern; two passes needed here
                             "%l" "%F : %M (%L)"); since this one is recursive
     #"%M|%F|%L|%u"
     {"%M" "%M" ; dunno how to find method yet, %M just gives us "write_BANG_"
      "%F" ((fnil identity "") file) ; file will be a string or nil
      "%L" (str line)
      "%u" (str user)}))) ; line will be a number or nil

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
                           (unwrap-conf (:file ~map) (:line ~map)
                                        (:user ~map)))]
     (logconf/with-logging-config
       [:root total#]
       ~@body)))

(defmacro whenlog
  "run body when level is enabled"
  [level & body]
  `(with-config {}
     (if (logging/enabled? ~level)
       ~@body)))

(defmacro loglevel
  [level msg line file prefix]
  `(with-config {:line ~line :file ~file :user ((fnil name "") (first ~prefix))}
     (~level ~msg)))

(defmacro debug
  "Log a debug message (with an optional prefix)"
  [msg & prefix]
  `(loglevel logging/debug ~msg ~(:line (meta &form))  ~*file* '~prefix))

(defmacro info
  "Log an info message (with an optional prefix)"
  [msg & prefix]
  `(loglevel logging/info ~msg ~(:line (meta &form))  ~*file* '~prefix))

(defmacro warn
  "Log a warning message (with an optional prefix)"
  [msg & prefix]
  `(loglevel logging/warn ~msg ~(:line (meta &form))  ~*file* '~prefix))

(defmacro error
  "Log an error message (with an optional prefix)"
  [msg & prefix]
  `(loglevel logging/error ~msg ~(:line (meta &form))  ~*file* '~prefix))

(defmacro spy
  "Spy the value of an expression (with an optional prefix)"
  [form & prefix]
  `(logconf/with-logging-config
       {:line ~(:line (meta &form))
        :file ~*file*
        :log-pattern (if (nil? (first '~prefix))
                       (:log-pattern @config/app)
                       (str (name (first '~prefix)) " "
                            (:log-pattern @config/app)))}
       (logging/spy '~form)))
