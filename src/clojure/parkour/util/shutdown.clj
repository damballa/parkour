(ns parkour.util.shutdown
  {:private true}
  (:require [parkour.util :refer [returning]]))

(def ^:private hooks
  (atom #{}))

(defn ^:private run-hooks
  [] (dorun (map #(%) @hooks)))

(defonce ^:private ^Thread hook-thread
  (doto (Thread. #'run-hooks)
    (as-> t (-> (Runtime/getRuntime) (.addShutdownHook t)))))

(defn add-hook
  "Arrange to run function `f` during JVM shutdown."
  [f] (returning true (swap! hooks conj f)))

(defn remove-hook
  "Remove arrangements made to run function `f` during JVM shutdown."
  [f] (returning true (swap! hooks disj f)))

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
