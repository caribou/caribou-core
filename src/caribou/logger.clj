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

(def socket (new DatagramSocket))

(def levels
  {:emergency 0
   :alert 1
   :critical 2
   :error 3
   :warning 4
   :warn 4
   :notice 5
   :informational 6
   :info 6
   :debug 7
   :trace 7})

(defn syslog
  [host]
  (let [dest (cond (string? host) (. InetAddress getByName host)
                   (sequential? host) (. InetAddress getByAddress
                                         (byte-array (map byte host)))
                   true (. InetAddress getLocalHost))]
    (fn [type message]
      (let [facility 1 ; user level
            severity (get levels type 7)
            priority (+ (* facility 8) severity)
            message (str "<" priority ">" " " (string/upper-case (name type))
                         " " message)
            packet (new DatagramPacket
                        (. message getBytes)
                        (. message length)
                        dest
                        514)]
        (. socket send packet)))))

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

(defn stdoutlog
  [level message]
  (println level message))

(def loggers (atom [[7 stdoutlog]]))

(def level (atom 7))

(defn make-logger
  [spec]
  (case (:type spec)
    :remote (syslog (:host spec))
    :stdout stdoutlog
    :file (filelog (:file spec))
    :default (constantly nil)))

(defn init
  [log-specs]
  (let [log (map (fn [s] [(get levels (:level s) 7)
                          (make-logger s)])
                 log-specs)
        lev (apply max (map (fn [x] (get levels (:level x) 7)) log-specs))]
    (swap! level (constantly lev))
    (swap! loggers (constantly log))))

(defmacro log
  [at-level message]
  `(when (>= (deref level) (get levels ~at-level 7))
     (let [ms# ~message]
       (map (fn [lv#]
              (when (>= (nth lv# 0) (get levels ~at-level 7))
                ((nth lv# 1) ~at-level ms#)))
            (deref loggers)))))

(defmacro debug
  [message & prefix]
  `(log :debug (str (first '~prefix) " " ~message)))

(defmacro info
  [message & prefix]
  `(log :info (str (first '~prefix) " " ~message)))

(defmacro warn
  [message & prefix]
  `(log :warn (str (first '~prefix) " " ~message)))

(defmacro error
  [message & prefix]
  `(log :error (str (first '~prefix) " " ~message)))

(defmacro spy-str
  "spy the value of an expression"
  [form]
  `(str '~form " : " ~form))

(defmacro spy
  [form]
  `(debug (spy-str ~form)))

