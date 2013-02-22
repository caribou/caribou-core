(ns caribou.callgraph
  (:require [swank.commands.xref :as xref]))

(def model-ops
  {:prefixes ["" "model" "caribou.model"]
   :functions ["gather"
               "pick"
               "create"
               "update"
               "destroy"]})

(defn make-search
  [ops]
  (mapcat
   (fn [name]
     (doall
      (map symbol (map #(str % (if (empty? %) "" "/") name) (:prefixes ops)))))
   (:functions ops)))

(defn callers
  [relevant]
  (let [calls (map (fn [called] {:name called
                                 :callers (filter identity
                                                  (map #(-> % str (subs 2))
                                                       (xref/all-vars-who-call
                                                        called)))})
                   relevant)
        calls (filter (comp seq :callers)
                      calls)]
    calls))

(defn make-prefixes
  [ns-string]
  (let [separator? (fn [c] (= (count c) 1))
        dot? (fn [c] (= c \.))
        ns-vecs (remove separator? (partition-by dot? ns-string))
        vec->str (partial apply str)
        ns-parts (map vec->str ns-vecs)]
    (loop [strings [""]
           parts (reverse ns-parts)
           path ""]
      (if (empty? parts)
        strings
        (let [part (first parts)
              path (if (empty? path) part (str part "." path))
              strings (conj strings path)]
          (recur strings (rest parts) path))))))

(defn ns->search
  [search-string]
  (let [[ns-part fn-part] (split-with #(not (= \/ %)) search-string)
        ns-string (apply str ns-part)
        fn-string (apply str (rest fn-part))] ; removing the \/
    (make-search {:prefixes (make-prefixes ns-string)
                  :functions [fn-string]})))

;; (defn usages
;;   []
;;   (let [usage (callers (make-search model-ops))
;;         usages (into {} (map (fn [{name :name _callers :callers}]
;;                                        [(keyword name)
;;                                         (filter seq (map
;;                                                      (comp callers ns->search)
;;                                                      _callers))])
;;                              usage))]
;;     usages))

(defn calls
  ([names depth]
     (calls names {} depth))
  ([names result depth]
     (if (<= depth 0)
       result
       (let [only-unsearched (fn [n] (not (get result (keyword n))))
             names (filter only-unsearched names)
             names (map (fn [name] [name (ns->search name)]) names)
             usage (map (fn [[name aliases]]
                          [name (callers aliases)])
                        names)
             result (reduce (fn [res [k v]] (assoc res k v)) result usage)
             concat-callers (fn [c] (mapcat :callers c))
             usages (mapcat (comp concat-callers second) usage)]
         (conj result
               (calls usages result (dec depth)))))))
