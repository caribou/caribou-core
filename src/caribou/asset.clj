(ns caribou.asset
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [aws.sdk.s3 :as s3]
            [pantomime.mime :as mime]
            [caribou.util :as util]
            [caribou.config :as config])
  (:import java.io.ByteArrayInputStream))

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

(defn s3-prefix
  []
  (if-let [prefix (:asset-prefix @config/app)]
    (str prefix "/")
    ""))

(defn asset-path
  "Construct the path this asset will live in, or look it up."
  [asset]
  (if (:asset-bucket @config/app)
    (if (and asset (:filename asset))
      (str "https://" (:asset-bucket @config/app) ".s3.amazonaws.com/"
           (s3-prefix) (asset-location asset))
      "")
    (asset-location asset)))

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

(defn persist-asset-on-disk
  [dir path file]
  (.mkdirs (io/file (util/pathify [(@config/app :asset-dir) dir])))
  (io/copy file (io/file (util/pathify [(@config/app :asset-dir) path]))))

(defn migrate-dir-to-s3
  [dir]
  (let [prefix (s3-prefix)
        dir-pattern (re-pattern (str dir "/"))]
    (doseq [entry (file-seq (io/file dir))]
      (if-not (.isDirectory entry)
        (let [path (.getPath entry)
              relative (string/replace-first path dir-pattern "")
              prefixed (str prefix relative)]
          (println "uploading" prefixed (.length entry))
          (upload-to-s3 prefixed (io/file path)))))))