(ns parkour.conf
  (:refer-clojure :exclude [assoc! get])
  (:require [clojure.string :as str]
            [parkour.wrapper :as w]
            [parkour.util :refer [returning]])
  (:import [java.util List Map Set]
           [org.apache.hadoop.conf Configuration Configurable]
           [org.apache.hadoop.mapreduce Job JobContext]))

(def ^:dynamic ^:private *default*
  "Base configuration, used as template for fresh configurations."
  (Configuration.))

(defmulti ^:private configuration*
  "Internal implementation multimethod for extracting a `Configuration`."
  type)

(defmulti ^:private conf-set*
  "Internal implementation multimethod for setting value in Hadoop config."
  (fn [_ _ val] (type val)))

(defmulti ^:private conf-coerce
  "Internal implementation multimethod for converting value to one
which may be set as Hadoop config value."
  type)

(defn configuration
  "Extract or produce a Hadoop `Configuration`.  If provided a `conf`
argument, coerce it to be a `Configuration` which if possible shares
mutable state with that original argument.  If not, return a fresh
configuration copied from the current default."
  {:tag `Configuration}
  ([] (Configuration. *default*))
  ([conf] (configuration* conf)))

(def ^{:tag `Configuration} iguration
  "Alias for `configuration`."
  configuration)

(def ^{:tag `Configuration} ig
  "Alias for `configuration`."
  configuration)

(defn clone
  "Return new Hadoop `Configuration`, cloning `conf`."
  [conf] (Configuration. (configuration conf)))

(defmacro with-default
  "Set new default configuration `conf` within the dynamic scope of
the `body` expressions."
  [conf & body]
  `(binding [*default* (clone ~conf)]
     ~@body))

(defn get
  "Get string value of `conf` parameter `key`"
  {:tag `String}
  ([conf key] (.get (configuration conf) key))
  ([conf key default] (.get (configuration conf) key default)))

(defn get-boolean
  "Get boolean value of `conf` parameter `key`"
  ^Boolean [conf key default] (.getBoolean (configuration conf) key default))

(defn get-int
  "Get int value of `conf` parameter `key`"
  ^Integer [conf key default] (.getInt (configuration conf) key default))

(defn get-long
  "Get long value of `conf` parameter `key`"
  ^Long [conf key default] (.getLong (configuration conf) key default))

(defn get-float
  "Get float value of `conf` parameter `key`"
  ^Float [conf key default] (.getFloat (configuration conf) key default))

(defn set!
  "Set `conf` parameter `key` to `val`."
  [conf key val]
  (returning conf
    (conf-set* (configuration conf) (name key) (conf-coerce val))))

(defn merge!
  "Merge `coll` of key-value pairs into Hadoop configuration `conf`."
  [conf coll]
  (returning conf
    (doseq [:let [conf (configuration conf)]
            [k v] coll]
      (parkour.conf/set! conf k v))))

(defn assoc!
  "Merge key-value pairs `kvs` into Hadoop configuration `conf`."
  [conf & kvs] (merge! conf (partition 2 kvs)))

(defmethod configuration* Configuration [conf] conf)
(defmethod configuration* Configurable
  [cable] (.getConf ^Configurable cable))
(defmethod configuration* JobContext
  [context] (.getConfiguration ^JobContext context))
(defmethod configuration* Job [job] (.getConfiguration ^Job job))
(defmethod configuration* Map [map] (merge! (configuration) map))
(defmethod configuration* :default [_] nil)

(defmacro ^:private def-conf-set*
  [[conf key val] & pairs]
  (let [conf (vary-meta conf assoc :tag `Configuration)
        key (vary-meta key assoc :tag `String)]
    `(do ~@(map (fn [[type form]]
                  (let [val (vary-meta val assoc :tag type)]
                    `(defmethod conf-set* ~type ~[conf key val]
                       (let [~conf (w/unwrap ~conf)]
                         ~form))))
                (partition 2 pairs)))))

(def-conf-set* [conf key val]
  String  (.set conf key val)
  Integer (.setInt conf key (int val))
  Long    (.setLong conf key (long val))
  Float   (.setFloat conf key (float val))
  Double  (.setFloat conf key (float val))
  Boolean (.setBoolean conf key (boolean val)))

(defmethod conf-coerce :default [val] val)
(defmethod conf-coerce Class [val] (.getName ^Class val))
(defmethod conf-coerce List [val] (str/join "," (map conf-coerce val)))
(defmethod conf-coerce Set [val] (str/join "," (map conf-coerce val)))
(defmethod conf-coerce Map [val]
  (let [kv-str (fn [[k v]] (str (conf-coerce k) "=" (conf-coerce v)))]
    (str/join "," (map kv-str val))))
