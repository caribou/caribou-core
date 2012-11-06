(defproject antler/caribou-core "0.7.7"
  :description "Caribou is a dynamic web application generator with antlers."
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [postgresql/postgresql "8.4-702.jdbc4"]
                 [com.h2database/h2 "1.3.154"]
                 [mysql/mysql-connector-java "5.1.6"]
                 [clj-time "0.4.4"]
                 [clj-yaml "0.3.1"]
                 [geocoder-clj "0.0.6"]
                 [org.clojure/core.cache "0.6.2"]
                 [org.clojure/tools.logging "0.2.3"]
                 [com.novemberain/pantomime "1.4.0"]
                 [clj-aws-s3 "0.3.2"]
                 [clj-logging-config "1.9.8"]]
  :dev-dependencies [[lein-autodoc "0.9.0"]]
  :jvm-opts ["-agentlib:jdwp=transport=dt_socket,server=y,suspend=n"]
  :autodoc {:name "Caribou Core"
            :page-title "Caribou Core - Documentation"
            :description
          "<p>This library represents the core model and configuration for Caribou.
           Used alone, it is a powerful way to capture your data model *as data*,
           effectively shaping it as your application develops.  Once the data
           model is rendered in this malleable form, Caribou can harness that
           specification to do a variety of things, including constructing
           queries based on the relationships *between* models you define.
           This means it is possible to order or filter results based on
           conditions that exist in associated models!</p>

           <p>Also, this provides the basis for the other Caribou libraries,
           <a href=\"http://antler.github.com/caribou-frontend\">Caribou Frontend</a>
           <a href=\"http://antler.github.com/caribou-api\">Caribou API</a> and 
           <a href=\"http://antler.github.com/caribou-admin\">Caribou Admin</a>,
           each of which build in their own way upon this data model as a basis."}
  :aot [caribou.model])
