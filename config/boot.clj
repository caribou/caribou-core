(use '[caribou.config :only (read-config configure)])

(def default-config
  {:debug        true
   :use-database true
   :halo-enabled true
   :halo-prefix "/_halo"
   :halo-key    "replace-with-halo-key"
   :halo-host   "http://127.0.0.1:33333"
   :database {:classname    "org.postgresql.Driver"
              :subprotocol  "postgresql"
              :host         "localhost"
              :database     "caribou_development"
              :user         "postgres"
              :password     ""}
   :template-dir   "site/resources/templates"
   :public-dir     "site/resources/public"
   :asset-dir      "app/"
   :hooks-dir      "app/hooks"
   :api-public     "api/resources/public"
   :controller-ns  "skel.controllers"})

(defn submerge
  [a b]
  (if (string? b) b (merge a b)))

(defn get-config
  []
  (merge-with submerge default-config (read-config "config/development.clj")))

;; This call is required by Caribou
(configure (get-config))
