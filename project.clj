(defproject caribou/caribou-core "0.15.3"
  :description "Caribou is a dynamic web application generator with antlers"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.codec "0.1.0"]
                 [org.clojure/java.jdbc "0.3.6"
                  :exclusions [org.clojure/clojure]]
                 [postgresql/postgresql "8.4-702.jdbc4"]
                 [com.h2database/h2 "1.3.170"]
                 [mysql/mysql-connector-java "5.1.6"]
                 [caribou/antlers "0.6.1"]
                 [clj-time "0.4.4"
                  :exclusions [org.clojure/clojure]]
                 [slingshot "0.10.3"
                  :exclusions [org.clojure/clojure]]
                 [geocoder-clj "0.2.3"
                  :exclusions [org.clojure/clojure slingshot]]
                 [org.clojure/tools.logging "0.2.3"
                  :exclusions [org.clojure/clojure]]
                 [com.novemberain/pantomime "2.0.0"
                  :exclusions [org.clojure/clojure]]
                 [leiningen-core "2.3.4"
                  :exclusions [org.clojure/clojure]]
                 [clj-aws-s3 "0.3.6"]
                 [clojure-complete "0.2.3"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 [clucy "0.3.1"]
                 [me.raynes/fs "1.4.5"]
                 [org.mindrot/jbcrypt "0.3m"]]
  :plugins [[lein-marginalia "0.7.1"]]
  :repositories [["releases" {:url "https://clojars.org/repo" :creds :gpg}]]
  :jvm-opts ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n" "-Xmx2g"]
  :test-selectors {:default (constantly true)
                   :mysql :mysql
                   :postgres :postgres
                   :h2 :h2
                   :non-db :non-db})
