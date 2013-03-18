{:logging {:loggers [{:type :stdout :level :debug}
                     ;; {:type :remote :host "beast.local" :level :debug}
                     ;; {:type :file :file "caribou-logging.out" :level :debug}
                     ]}
 :database {:classname		"org.h2.Driver"
            :subprotocol	"h2"
            :subname		"mem:test"
            :user		"h2"
            :password		""
            :max-count		1}}

