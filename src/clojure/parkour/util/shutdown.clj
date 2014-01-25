(ns parkour.util.shutdown
  {:private true}
  (:require [parkour.util :refer [returning compile-if]]))

(compile-if org.apache.hadoop.util.ShutdownHookManager
  (require '[parkour.util.shutdown.hadoop2 :as impl])
  (require '[parkour.util.shutdown.hadoop1 :as impl]))

(defn add-hook
  "Arrange to run function `f` during JVM shutdown."
  [f] (returning true (impl/add-hook f)))

(defn remove-hook
  "Remove arrangements made to run function `f` during JVM shutdown."
  [f] (returning true (impl/remove-hook f)))

(defmacro with-hook
  "Execute `body` with function `f` as a registered shutdown hook for `body`'s
dynamic scope."
  [f & body]
  `(let [f# ~f]
     (try
       (add-hook f#)
       ~@body
       (finally
         (remove-hook f#)))))
