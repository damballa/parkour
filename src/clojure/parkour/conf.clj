(ns parkour.conf
  (:refer-clojure :exclude [assoc! get])
  (:require [clojure.string :as str]
            [clojure.reflect :as reflect]
            [parkour.util :refer [returning]])
  (:import [java.io Writer]
           [java.util List Map Set]
           [org.apache.hadoop.conf Configuration Configurable]
           [org.apache.hadoop.fs Path]
           [org.apache.hadoop.mapreduce Job JobContext]))

(def ^:dynamic ^:private ^Configuration *default*
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
  {:tag `Configuration}
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
  "Get boolean value of `conf` parameter `key`."
  ^Boolean [conf key default] (.getBoolean (configuration conf) key default))

(defn get-int
  "Get int value of `conf` parameter `key`."
  ^Integer [conf key default] (.getInt (configuration conf) key default))

(defn get-long
  "Get long value of `conf` parameter `key`."
  ^Long [conf key default] (.getLong (configuration conf) key default))

(defn get-float
  "Get float value of `conf` parameter `key`."
  ^Float [conf key default] (.getFloat (configuration conf) key default))

(defn get-class
  "Get class value of `conf` parameter `key`."
  ^Class [conf key default] (.getClass (configuration conf) key default))

(defn get-vector
  "Get string-vector value of `conf` parameter `key`."
  [conf key default] (or (some-> (get conf key) (str/split #",")) default))

(defn assoc!
  "Set `conf` parameter `key` to `val`."
  {:tag `Configuration}
  ([conf] conf)
  ([conf key val]
     (returning conf
       (conf-set* (configuration conf) (name key) (conf-coerce val))))
  ([conf key val & kvs]
     (let [conf (assoc! conf key val)]
       (if (empty? kvs)
         conf
         (recur conf (first kvs) (second kvs) (nnext kvs))))))

(def ^{:tag `Configuration} set!
  "Alias for `assoc!`."
  assoc!)

(defn merge!
  "Merge `coll` of key-value pairs into Hadoop configuration `conf`."
  {:tag `Configuration}
  [conf coll]
  (apply assoc! conf (apply concat coll)))

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
                       ~form)))
                (partition 2 pairs)))))

(def ^:private unset-method?
  (->> Configuration reflect/type-reflect :members
       (some #(= 'unset (:name %)))))

(defmacro ^:private conf-unset
  [conf key] (if unset-method? `(. ~conf unset ~key)))

(def-conf-set* [conf key val]
  nil     (conf-unset conf key)
  String  (.set conf key val)
  Integer (.setInt conf key (int val))
  Long    (.setLong conf key (long val))
  Float   (.setFloat conf key (float val))
  Double  (.setFloat conf key (float val))
  Boolean (.setBoolean conf key (boolean val)))

(defmethod conf-coerce :default [val] val)
(defmethod conf-coerce Path [val] (str val))
(defmethod conf-coerce Class [val] (.getName ^Class val))
(defmethod conf-coerce List [val] (str/join "," (map conf-coerce val)))
(defmethod conf-coerce Set [val] (str/join "," (map conf-coerce val)))
(defmethod conf-coerce Map [val]
  (let [kv-str (fn [[k v]] (str (conf-coerce k) "=" (conf-coerce v)))]
    (str/join "," (map kv-str val))))

(defn diff
  "Map of updates from `conf` to `conf'`."
  ([conf'] (diff *default* conf'))
  ([conf conf']
     (let [conf (configuration conf), conf' (configuration conf')]
       (reduce (fn [diff key]
                 (let [val (get conf key), val' (get conf' key)]
                   (if (= val val')
                     diff
                     (assoc diff key val'))))
               {} (distinct (mapcat keys [conf conf']))))))

(defn copy!
  "Copy all configuration from `conf'` to `conf`."
  [conf conf'] (merge! conf (diff conf conf')))

(defmethod print-method Configuration
  [conf ^Writer w]
  (.write w "#hadoop.conf/configuration ")
  (print-method (diff conf) w))
