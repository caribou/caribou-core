(ns caribou.test.assets
  (:require [caribou.asset :as asset]
            [aws.sdk.s3 :as s3]
            [clojure.test :as test :refer [is testing deftest]]
            [caribou.config :as config]
            [clojure.java.io :as io]))

;;; integration test of our s3 compatibility

(deftest sanity
  (is true))

(defn to-stream [s] (io/input-stream (.getBytes s)))

(defn get-config
  []
  (config/read-config (io/resource "config/test-aws.clj")))

(def payload "HELLO WORLD!")

(defn construct-loc
  "for s3 level tests, so we can separate s3 issues from caibou issues"
  [prefix fname]
  (str prefix "/" (asset/asset-dir {}) "/" fname))

(defn upload-download
  [upload]
  (let [config (get-config)
        aws (:aws config)
        creds (:credentials aws)
        existing (s3/list-objects creds (:bucket aws))
        prefix (-> config :assets :prefix)
        prefixed (fn [s] (and (string? s) (.startsWith s (str prefix))))
        test-uploads (fn [objects] (filter (comp prefixed :key) objects))
        enumerated (count (test-uploads (:objects existing)))
        fname (str "test-content-" (.getTime (java.util.Date.)))
        _ (upload prefix fname (to-stream payload) config)
        incremented (s3/list-objects creds (:bucket aws))
        expanded (count (test-uploads (:objects incremented)))
        contents (try (slurp (str "http://" (:bucket aws) ".s3.amazonaws.com/"
                                  (construct-loc prefix fname)))
                      (catch Throwable t t))]
    (testing "did something get uploaded?"
      (is (= expanded (inc enumerated))))
    (testing "does it have the expected contents?"
      (is (= payload contents)))
    (testing "delete that shit"
      (is (nil?
           (s3/delete-object creds (:bucket aws)
                             (construct-loc prefix fname)))))))

(deftest s3-level
  (upload-download
   (fn [prefix fname stream config]
     (s3/put-object (:credentials (:aws config))
                    (:bucket (:aws config))
                    (construct-loc prefix fname)
                    stream
                    {:content-type "text/plain"
                     :content-length (count payload)}
                    (s3/grant :all-users :read)))))

(deftest caribou-asset-level
  (upload-download
   (fn [prefix fname stream config]
     (config/with-config config
       (let [asset {:filename fname :tempfile stream}
             location (asset/asset-upload-path asset)]
         (asset/upload-to-s3 location stream (count payload)))))))
