(ns parkour.cser.printers
  (:import [java.io Writer]
           [java.util.regex Pattern]
           [clojure.lang Var]))

(defn class-print
  "Tagged literal printer for Java classes."
  [^Class x ^Writer w]
  (doto w
    (.append "#parkour/class ")
    (.append (.getName x))))

(defn var-print
  "Tagged literal printer for Clojure vars."
  [^Var x ^Writer w]
  (doto w
    (.append "#parkour/var ")
    (.append (str (.-ns x)))
    (.append \/)
    (.append (str (.-sym x)))))

(defn pattern-print
  "Tagged literal printer for Java compiled regular expressions."
  [^Pattern x ^Writer w]
  (doto w
    (.append "#parkour/pattern \"")
    (.append (str x))
    (.append \")))

(def data-printers
  "Custom data printers for cser printing."
  {Class class-print,
   Var var-print,
   Pattern pattern-print,
   })
