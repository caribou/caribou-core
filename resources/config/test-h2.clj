{:logging {:loggers [{:type :stdout :level :debug}
                     ;; {:type :remote :host "beast.local" :level :debug}
                     ;; {:type :file :file "caribou-logging.out" :level :debug}
                     ]}
 :db {:database {:classname    "org.h2.Driver"
                 :subprotocol  "h2"
                 :protocol     "file"
                 :path         "/tmp/"
                 :database     "caribou_development"
                 :user         "h2"
                 :password     ""}}}

