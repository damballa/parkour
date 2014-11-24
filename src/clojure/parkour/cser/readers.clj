(ns parkour.cser.readers
  (:import [java.util.regex Pattern]
           [clojure.lang RT Var]))

(defn class-read
  "Tagged literal reader for Java classes."
  [x] (Class/forName (name x) false (RT/baseLoader)))

(defn var-read
  "Tagged literal reader for Clojure vars."
  [x]
  (let [ns (symbol (namespace x))
        n (symbol (name x))]
    (require ns)
    (intern ns n)))

(defn pattern-read
  "Tagged literal reader for Java compiled regular expressions."
  [x] (Pattern/compile x))

(def data-readers
  "Custom data readers for cser reading."
  {'parkour/class class-read,
   'parkour/var var-read,
   'parkour/pattern pattern-read,
   })
