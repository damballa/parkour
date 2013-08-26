(ns parkour.conf
  (:require [clojure.string :as str]
            [parkour.util :refer [returning]])
  (:import [java.util List Map Set]
           [org.apache.hadoop.conf Configuration]))

(defmulti ^:private conf-set*
  "Internal implementation multimethod for setting value in Hadoop config."
  (fn [_ _ val] (type val)))

(defmacro def-conf-set*
  [[conf key val] & pairs]
  (let [conf (vary-meta conf assoc :tag `Configuration)
        key (vary-meta key assoc :tag `String)]
    `(do ~@(map (fn [[type form]]
                  (let [val (vary-meta val assoc :tag type)]
                    `(defmethod conf-set* ~type ~[conf key val] ~form)))
                (partition 2 pairs)))))

(def-conf-set* [conf key val]
  String  (.set conf key val)
  Integer (.setInt conf key (int val))
  Long    (.setLong conf key (long val))
  Float   (.setFloat conf key (float val))
  Double  (.setFloat conf key (float val))
  Boolean (.setBoolean conf key (boolean val)))

(defmulti ^:private conf-coerce
  "Internal implementation multimethod for converting value to one
which may be set as Hadoop config value."
  type)

(defmethod conf-coerce :default [val] val)
(defmethod conf-coerce Class [val] (.getName ^Class val))
(defmethod conf-coerce List [val] (str/join "," (map conf-coerce val)))
(defmethod conf-coerce Set [val] (str/join "," (map conf-coerce val)))
(defmethod conf-coerce Map [val]
  (let [kv-str (fn [[k v]] (str (conf-coerce k) "=" (conf-coerce v)))]
    (str/join "," (map kv-str val))))

(defn conf-set!
  "Set `conf` parameter `key` to `val`."
  [conf key val]
  (returning conf
    (conf-set* conf (name key) (conf-coerce val))))

(defn conf-merge!
  "Merge `coll` of key-value pairs into Hadoop configuration `conf`."
  [conf coll]
  (returning conf
    (doseq [[k v] coll] (conf-set! conf k v))))
