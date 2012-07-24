(ns caribou.debug
  (:require [caribou.logger :as log]))

(defmacro debug
  "Simple way to print the value of an expression while still evaluating to the
   same thing.  Example:  (debug (inc 3)) --> 4  *prints 4*"
  [x]
  `(let [x# ~x] (log/debug (str '~x " -> " x#)) x#))

(defmacro log
  "Same as debug but takes a key that illustrates what conceptual area this
   logged information belongs to."
  [key x]
  `(let [x# ~x] (log/debug (str '~x " -> " x#) ~key) x#))

(defmacro local-bindings
  "Produces a map of the names of local bindings to their values."
  []
  (let [symbols (map key @clojure.lang.Compiler/LOCAL_ENV)]
    (zipmap (map (fn [sym] `(quote ~sym)) symbols) symbols)))

(declare ^:dynamic *locals*)
(defn eval-with-locals
  "Evals a form with given locals.  The locals should be a map of symbols to
  values."
  [locals form]
  (binding [*locals* locals]
    (eval
     `(let ~(vec (mapcat #(list % `(*locals* '~%)) (keys locals)))
        ~form))))

(defmacro repl
  "Starts a REPL with the local bindings available."
  []
  `(clojure.main/repl
    :prompt #(print "debug => ")
    :eval (partial eval-with-locals (local-bindings))))

