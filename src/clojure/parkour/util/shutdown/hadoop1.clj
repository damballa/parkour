(ns parkour.util.shutdown.hadoop1
  {:private true}
  (:require [parkour.util :refer [ignore-errors returning]])
  (:import [java.lang.reflect Field]
           [java.util Map]
           [java.util.concurrent LinkedBlockingDeque]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileSystem FileSystem$Cache]))

(def ^:private ^LinkedBlockingDeque
  hooks
  (LinkedBlockingDeque.))

(defn ^:private run-hooks
  [] (dorun (map #(%) hooks)))

(defn add-hook
  "Arrange to run function `f` during JVM shutdown."
  [f] (.addFirst hooks f))

(defn remove-hook
  "Remove arrangements made to run function `f` during JVM shutdown."
  [f] (.remove hooks f))

(defn ^:private field-get
  ([cls fname] (field-get cls fname nil))
  ([cls fname obj]
     (-> (doto (.getDeclaredField ^Class cls ^String fname)
           (.setAccessible true))
         (.get obj))))

(defonce ^:private ^Thread hook-thread
  (let [fs-cache (field-get FileSystem "CACHE")]
    (locking fs-cache
      (let [m ^Map (field-get FileSystem$Cache "map" fs-cache)
            t' ^Thread (or (ignore-errors
                            (field-get FileSystem "clientFinalizer"))
                           (field-get FileSystem$Cache "clientFinalizer"
                                      fs-cache))
            t (Thread. #'run-hooks)]
        (returning t
          (.put m nil nil) ;; Prevent cache from ever being empty
          (when t'
            (add-hook #(some-> t' .start))
            (.removeShutdownHook (Runtime/getRuntime) t'))
          (.addShutdownHook (Runtime/getRuntime) t))))))
