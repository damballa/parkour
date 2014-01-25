(ns parkour.util.shutdown.hadoop2
  {:private true}
  (:require [parkour.util :refer [returning]])
  (:import [org.apache.hadoop.fs FileSystem]
           [org.apache.hadoop.util ShutdownHookManager]))

(def ^:private hook-priority
  (inc FileSystem/SHUTDOWN_HOOK_PRIORITY))

(defn add-hook
  "Arrange to run function `f` during JVM shutdown."
  [f] (.addShutdownHook (ShutdownHookManager/get) f hook-priority))

(defn remove-hook
  "Remove arrangements made to run function `f` during JVM shutdown."
  [f] (.removeShutdownHook (ShutdownHookManager/get) f))
