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
  (util/pathify ["assets" (pad-break-id (:id asset))]))

(defn asset-location
  [asset]
  (if (and asset (:filename asset))
    (util/pathify
     [(asset-dir asset)
      ;; this regex is to deal with funky windows file paths
      (re-find
       #"[^:\\\/]*$"
       (:filename asset))])
    ""))

(defn s3-prefix
  ([]
     (s3-prefix (config/draw :assets :prefix)))
  ([prefix]
     (if prefix
       (str prefix "/")
       "")))

(defn s3-key
  [asset]
  (str (s3-prefix) (asset-location asset)))

(defn asset-path
  "Where to look to find this asset."
  [asset]
  (if (config/draw :aws :bucket)
    (if (and asset (:filename asset))
      (str "https://" (config/draw :aws :bucket) ".s3.amazonaws.com/"
           (s3-key asset))
      "")
    (asset-location asset)))

(defn asset-upload-path
  "Where to send this asset to."
  [asset]
  (if (config/draw :aws :bucket)
    (if (and asset (:filename asset))
      (s3-key asset)
      "")
    (asset-path asset)))

(defn ensure-s3-bucket
  [cred bucket]
  (if-not (s3/bucket-exists? cred bucket)
    (do
      (s3/create-bucket cred bucket)
      (s3/update-bucket-acl cred bucket (s3/grant :all-users :read)))))

(defn upload-to-s3
  ([key asset size]
     (upload-to-s3 (config/draw :aws :bucket) key asset size))
  ([bucket key asset size]
     (try
       (let [cred (config/draw :aws :credentials)
             mime (mime/mime-type-of key)]
         (ensure-s3-bucket cred bucket)
         (s3/put-object cred bucket key asset
                        {:content-type mime
                         :content-length size}
                        (s3/grant :all-users :read)))
       (catch Exception e (do
                            (.printStackTrace e)
                            (println "KEY BAD" key))))))

(defn persist-asset-on-disk
  [dir path file]
  (.mkdirs (io/file (util/pathify [(config/draw :assets :dir) dir])))
  (io/copy file (io/file (util/pathify [(config/draw :assets :dir) dir path]))))

(defn put-asset
  [stream asset]
  (if (config/draw :aws :bucket)
    (if (and asset (:filename asset))
      (upload-to-s3 (asset-upload-path asset) stream (:size asset)))
    (persist-asset-on-disk (asset-upload-path asset) (:filename asset) stream)))

(defn migrate-dir-to-s3
  ([dir] (migrate-dir-to-s3 dir (config/draw :aws :bucket)))
  ([dir bucket] (migrate-dir-to-s3 dir bucket (config/draw :assets :prefix)))
  ([dir bucket prefix]
     (let [dir-pattern (re-pattern (str dir "/"))]
       (doseq [entry (file-seq (io/file dir))]
         (if-not (.isDirectory entry)
           (let [path (.getPath entry)
                 relative (string/replace-first path dir-pattern "")
                 prefixed (str (s3-prefix prefix) relative)]
             (println "uploading" prefixed (.length entry))
             (upload-to-s3 bucket prefixed (io/file path))))))))