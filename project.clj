(defproject antler/caribou-core "0.5.0"
  :description "caribou: type structure interaction api"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.0.6"]
                 [postgresql/postgresql "8.4-702.jdbc4"]
                 [clj-time "0.3.6"]
                 [clj-yaml "0.3.1"]
                 [geocoder-clj "0.0.3"]
                 [org.freemarker/freemarker "2.3.18"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.imgscalr/imgscalr-lib "4.2"]]
                 ;; [antler/clojure-solr "0.3.0-SNAPSHOT"]
                 ;; --------- THESE DEPS ARE NOT ON 1.3 -------------
                 ;; [aleph "0.2.0-rc2"]
                 ;; [org.clojars.serabe/rinzelight "0.0.4"]]
                 ;; -------------------------------------------------
  :jvm-opts ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :aot [caribou.model])
