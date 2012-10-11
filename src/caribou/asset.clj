(ns caribou.asset
  (:require [clojure.string :as string]
            [aws.sdk.s3 :as s3]
            [pantomime.mime :as mime]
            [caribou.util :as util]
            [caribou.config :as config]))

(defn- pad-break-id [id]
  (let [root (str id)
        len (count root)
        pad-len (- 8 len)
        pad (apply str (repeat pad-len "0"))
        halves (map #(apply str %) (partition 4 (str pad root)))
        path (string/join "/" halves)]
    path))

(defn asset-dir
  "Construct the dir this asset will live in, or look it up."
  [asset]
  (util/pathify ["assets" (pad-break-id (asset :id))]))

(defn asset-location
  [asset]
  (if (and asset (asset :filename))
    (util/pathify
     [(asset-dir asset)
      ;; this regex is to deal with funky windows file paths
      (re-find
       #"[^:\\\/]*$"
       (:filename asset))])
    ""))

(defn asset-path
  "Construct the path this asset will live in, or look it up."
  [asset]
  (if (and asset (asset :filename))
    (util/pathify
     ["https://s3.amazonaws.com"
      (:asset-bucket @config/app)
      (asset-location asset)])
    ""))

    ;;   (asset-dir asset)
    ;;   ;; this regex is to deal with funky windows file paths
    ;;   (re-find
    ;;    #"[^:\\\/]*$"
    ;;    (:filename asset))])
    ;; ""))

(defn ensure-s3-bucket
  [cred bucket]
  (if-not (s3/bucket-exists? cred bucket)
    (do
      (s3/create-bucket cred bucket)
      (s3/update-bucket-acl cred bucket (s3/grant :all-users :read)))))

(defn upload-to-s3
  [key asset]
  (let [cred (:aws-credentials @config/app)
        bucket (:asset-bucket @config/app)
        mime (mime/mime-type-of key)]
    (ensure-s3-bucket cred bucket)
    (s3/put-object cred bucket key asset {:content-type mime})
    (s3/update-object-acl cred bucket key (s3/grant :all-users :read))))

