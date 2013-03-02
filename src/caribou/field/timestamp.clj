(ns caribou.field.timestamp
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [clj-time.core :as timecore]
            [clj-time.format :as format]
            [clj-time.coerce :as coerce]
            [caribou.util :as util]
            [caribou.field :as field]
            [caribou.validation :as validation]))

(import java.util.Date)
(import java.text.SimpleDateFormat)

(defn current-timestamp
  []
  (coerce/to-timestamp (timecore/now)))

(def simple-date-format
  (java.text.SimpleDateFormat. "MMMMMMMMM dd', 'yyyy HH':'mm"))

(defn format-date
  "given a date object, return a string representing the canonical format for that date"
  [date]
  (if date
    (.format simple-date-format date)))

(def custom-formatters
  (map #(format/formatter %)
       ["MM/dd/yy"
        "MM/dd/yyyy"
        "MMMMMMMMM dd, yyyy"
        "MMMMMMMMM dd, yyyy HH:mm"
        "MMMMMMMMM dd, yyyy HH:mm:ss"
        "MMMMMMMMM dd yyyy"
        "MMMMMMMMM dd yyyy HH:mm"
        "MMMMMMMMM dd yyyy HH:mm:ss"]))

(def time-zone-formatters
  (map #(format/formatter %)
       ["MM/dd/yy Z"
        "MM/dd/yyyy Z"
        "MMMMMMMMM dd, yyyy Z"
        "MMMMMMMMM dd, yyyy HH:mm Z"
        "MMMMMMMMM dd, yyyy HH:mm:ss Z"
        "MMMMMMMMM dd yyyy Z"
        "MMMMMMMMM dd yyyy HH:mm Z"
        "MMMMMMMMM dd yyyy HH:mm:ss Z"]))

(defn try-formatter
  [date-string formatter]
  (try
    (format/parse formatter date-string)
    (catch Exception e nil)))

(defn impose-time-zone
  [timestamp]
  (timecore/from-time-zone timestamp (timecore/default-time-zone)))

(defn read-date
  "Given a string try every imaginable thing to parse it into something
   resembling a date."
  [date-string]
  (let [trimmed (string/trim date-string)
        default (coerce/from-string trimmed)]
    (if (nil? default)
      (let [custom (some #(try-formatter trimmed %) time-zone-formatters)]
        (if custom
          (coerce/to-timestamp custom)
          (let [custom (some #(try-formatter trimmed %) custom-formatters)]
            (if custom
              (coerce/to-timestamp (impose-time-zone custom))))))
      (coerce/to-timestamp (impose-time-zone default)))))

(defn ago
  "given a timecore/interval, creates a string representing the time passed"
  [interval]
  (let [notzero (fn [convert data]
                  (let [span (convert data)]
                    (if (== span 0)
                      false
                      span)))
        ago-str (fn [string num]
                  (str num " " string (if (== num 1) "s" "") " ago"))]
    (condp notzero interval
      timecore/in-years :>> #(ago-str "year" %)
      timecore/in-months :>> #(ago-str "month" %)
      timecore/in-days :>> #(ago-str "day" %)
      (constantly 1) (ago-str "hour" (timecore/in-hours interval)))))

(defn build-extract
  [field prefix slug opts [index value]]
  (let [model-id (-> field :row :model_id)
        model (@field/models model-id)
        field-select (field/coalesce-locale model field prefix slug opts)]
    (util/clause "extract(%1 from %2) = %3" [(name index) field-select value])))

(defn suffix-prefix
  [prefix]
  (if (or (nil? prefix) (empty? prefix))
    ""
    (str prefix ".")))

(def time-keys #{:second :minute :hour :day :month :year})
(def comparison-keys #{:> :>= := :< :<= :<>})

(defn timestamp-where
  "To find something by a certain timestamp you must provide a map with keys into
   the date or time.  Example:
     (timestamp-where :created_at {:day 15 :month 7 :year 2020})
   would find all rows who were created on July 15th, 2020."
  [field prefix slug opts where]
  (let [model-id (-> field :row :model_id)
        model (@field/models model-id)
        field-select (field/coalesce-locale model field prefix slug opts)]
    (if (map? where)
      (let [where-keys (set (keys where))
            extract-keys (set/intersection where-keys time-keys)
            op-keys (set/intersection where-keys comparison-keys)]
        (if (empty? op-keys)
          (string/join
           " and "
           (map
            (fn [[index value]]
              (util/clause "extract(%1 from %2) = %3" [(name index) field-select value]))
            (select-keys where extract-keys)))
          (string/join
           " and "
           (map
            (fn [[operator value]]
              (util/clause "%1 %2 '%3'" [field-select operator value]))
            (select-keys where op-keys)))))
      (util/clause "%1 = '%2'" [field-select where]))))

(defrecord TimestampField [row env]
  field/Field
  (table-additions [this field] [[(keyword field) "timestamp"]])
  (subfield-names [this field] [])
  (setup-field [this spec] nil)
  (rename-model [this old-slug new-slug])
  (rename-field [this old-slug new-slug])
  (cleanup-field [this]
    (field/field-cleanup this))
  (target-for [this] nil)
  (update-values
    [this content values]
    (let [key (keyword (:slug row))]
      (cond
       (contains? content key)
       (let [value (content key)
             timestamp (if (string? value) (read-date value) value)]
         (if timestamp
           (assoc values key timestamp)
           values))
       :else values)))
  (post-update [this content opts] content)
  (pre-destroy [this content] content)
  (join-fields [this prefix opts] [])
  (join-conditions [this prefix opts] [])
  (build-where
    [this prefix opts]
    (field/field-where this prefix opts timestamp-where))
  (natural-orderings [this prefix opts])
  (build-order [this prefix opts]
    (field/pure-order this prefix opts))
  (field-generator [this generators]
    generators)
  (fuse-field [this prefix archetype skein opts]
    (field/pure-fusion this prefix archetype skein opts))
  (localized? [this] (not (:locked row)))
  (models-involved [this opts all] all)
  (field-from [this content opts] (content (keyword (:slug row))))
  (render [this content opts]
    (update-in content [(keyword (:slug row))] format-date))
  (validate [this opts]
    (validation/for-type this opts
                         (fn [d]
                           (try
                             (new Date d)
                             (catch Throwable t false)))
                         "timestamp")))

(defn constructor
  [row]
  (TimestampField. row {}))

