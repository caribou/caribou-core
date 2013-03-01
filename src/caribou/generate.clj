(ns caribou.generate
  (:require [clojure.string :as string]))

(declare generate-where)

(defn generate-clause
  [[key val]]
  (let [subwhere (generate-where val)]
    (str "id in (select base_id from key where subwhere)")))

(defn generate-where
  [clause]
  (cond
   (map? clause) (string/join " and " (map generate-clause clause))))