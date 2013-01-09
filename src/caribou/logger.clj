(ns caribou.logger
  (:require [clojure.tools.logging :as logging]
            [clj-logging-config.log4j :as logconf]
            [clojure.string :as string]
            [clojure.java.io :as io])
  (:import java.util.Date
           java.net.DatagramSocket
           java.net.DatagramPacket
           java.net.InetAddress
           org.apache.log4j.PatternLayout
           org.apache.log4j.net.SyslogAppender))

(def defaults (ref
               {:log-pattern "%-5p %u [%t]: %m %F %L%n"
                :log-level :debug
                :log-filter (constantly true)
                :debug true}))

(def socket (new DatagramSocket))

(defn syslog
  [host]
  (let [dest (cond (string? host) (. InetAddress getByName host)
                   (sequential? host) (. InetAddress getByAddress
                                         (byte-array (map byte host)))
                   true (. InetAddress getLocalHost))]
    (fn [type message]
      (let [facility 1 ; user level
            severity (get {:emergency 0
                           :alert 1
                           :critical 2
                           :error 3
                           :warning 4
                           :warn 4
                           :notice 5
                           :informational 6
                           :info 6
                           :debug 7} type
                           7)
            priority (+ (* facility 8) severity)
            message (str "<" priority ">" " " (string/upper-case (name type))
                         " " message)
            packet (new DatagramPacket
                        (. message getBytes)
                        (. message length)
                        dest
                        514)]
        (. socket send packet)))))

(def loggers [])

(defn filelog
  [file]
  (let [writer (io/writer file :append true)]
    (fn [level message]
      (if (= level :close)
        (.close writer)
        (do (.write writer (str (Date.) " "
                                (string/upper-case (name level)) " "
                                message "\n"))
            (.flush writer))))))
            

(defn make-logger
  [spec]
  (case (:type spec)
    :remote (syslog (:host spec))
    :stdout (fn [level message] (println level message))
    :file (filelog (:file spec))
    :default (constantly nil)))


(defn init
  [log-specs]
  (let [loggers (map make-logger log-specs)]
    (def loggers loggers)))

(defn log
  [level message]
  (map (fn [l] (l level message)) loggers))

(defn debug
  [message & prefix]
  (log :debug (str prefix message)))

(defn info
  [message & prefix]
  (log :info (str prefix message)))

(defn warn
  [message & prefix]
  (log :warn (str prefix message)))

(defn error
  [message & prefix]
  (log :error (str prefix message)))

;; (defmacro spy
;;   "Spy the value of an expression (with an optional prefix)"
;;   [form & prefix]
;;   `(logconf/with-logging-config
;;        {:line ~(:line (meta &form))
;;         :file ~*file*
;;         :log-pattern (if (nil? (first '~prefix))
;;                        (:log-pattern @config/app)
;;                        (str (name (first '~prefix)) " "
;;                             (:log-pattern @config/app)))}
;;        (logging/spy '~form)))
