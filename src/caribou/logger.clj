(ns caribou.logger
  (:require [clojure.tools.logging :as logging]
            [clj-logging-config.log4j :as logconf]))

;; we should need the following call to have a logger, but right now invoking it
;; crashes at library load
;(logconf/set-logger!)

(def defaults (ref
               {:log-pattern "%-5p %u [%t]: %m %F %L%n"
                :log-level :debug
                :log-filter (constantly true)
                :debug true}))

(def logger-factory (atom :error-please-init-caribou.logger))

(defn unwrap-conf
  [file line user]
  (fn [pattern]
    (clojure.string/replace
     (clojure.string/replace pattern; two passes needed here
                             "%l" "%F : %M (%L)"); since this one is recursive
     #"%M|%F|%L|%u"
     {"%M" "%M" ; dunno how to find method yet, %M just gives us "write_BANG_"
      "%F" ((fnil identity "") file) ; file will be a string or nil
      "%L" (str line)  ; line will be a number or nil
      "%u" (str user)})))

;; adapted from clj-logging-config.log4j
(deftype ThreadLocalLog [old-log-factory log-ns ^org.apache.log4j.Logger logger]
  clojure.tools.logging.impl.Logger
  (enabled? [_ level]
    (or
     ;; Check original logger
     (clojure.tools.logging.impl/enabled?
      (clojure.tools.logging.impl/get-logger old-log-factory log-ns) level)
     ;; Check thread-local logger
     (.isEnabledFor logger
                    (or (logconf/log4j-levels level)
                        (throw (IllegalArgumentException. (str level)))))))
  (write! [_ level throwable message]
    ;; Write the message to the original logger on the thread
    (when-let [orig-logger
               (clojure.tools.logging.impl/get-logger old-log-factory log-ns)]
      (clojure.tools.logging.impl/write! orig-logger level throwable message))
    ;; Write the message to our thread-local logger
    (let [l (or
             (logconf/log4j-levels level)
             (throw (IllegalArgumentException. (str level))))]
      (if-not throwable
        (.log logger l message)
        (.log logger l message throwable)))))

(defn init
  []
  (let [old-log-factory logging/*logger-factory*
        thread-root-logger (org.apache.log4j.spi.RootLogger.
                            org.apache.log4j.Level/DEBUG)
        thread-repo (proxy [org.apache.log4j.Hierarchy]
                        [thread-root-logger]
                      (shutdown [] nil))
        defaults {:pattern ((unwrap-conf "" "" "")
                            (:log-pattern @defaults))
                  :level (:log-level @defaults)
                  :filter (:log-filter @defaults)}
        config [:root defaults]]
    (doall (map logconf/set-logger ; make this simpler since we are doing one
                (map (partial logconf/as-logger* thread-repo)
                     (partition 2 config))))
    (swap! logger-factory
           (constantly (reify clojure.tools.logging.impl.LoggerFactory
                         (name [_] "clj-logging-config.thread-local-logging")
                         (get-logger [_ log-ns]
                           (ThreadLocalLog.
                            old-log-factory log-ns
                            (.getLogger thread-repo
                                        ^String (str log-ns)))))))))

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

(defn logmsg
  [msg prefix]
  (str (if prefix (name (first prefix)) " ") "") msg)

(defn debug
  "Log a debug message (with an optional prefix)"
  [msg & prefix]
  (binding [clojure.tools.logging/*logger-factory* @logger-factory]
    (logging/debug (logmsg msg prefix))))

(defn info
  "Log an info message (with an optional prefix)"
  [msg & prefix]
  (binding [clojure.tools.logging/*logger-factory* @logger-factory]
    (logging/info (logmsg msg prefix))))

(defn warn
  "Log a warning message (with an optional prefix)"
  [msg & prefix]
  (binding [clojure.tools.logging/*logger-factory* @logger-factory]
    (logging/warn (logmsg msg prefix))))

(defn error
  "Log an error message (with an optional prefix)"
  [msg & prefix]
  (binding [clojure.tools.logging/*logger-factory* @logger-factory]
    (logging/error (logmsg msg prefix))))

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
