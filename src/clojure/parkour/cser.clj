(ns parkour.cser
  (:refer-clojure :exclude [assoc! get])
  (:require [parkour.conf :as conf])
  (:import [org.apache.hadoop.conf Configuration]
           [parkour.edn EdnReader]))

(def ^:internal ^:dynamic *conf*
  "Configuration of job being executed/configured during job configuration
de/serialization."
  nil)

(defn ^:private assoc!*
  "Internal implementation for `assoc!`."
  ([conf key val]
     (conf/assoc! conf key (pr-str val)))
  ([conf key val & kvs]
     (let [conf (assoc!* conf key val)]
       (if (empty? kvs)
         conf
         (recur conf (first kvs) (second kvs) (nnext kvs))))))

(defn assoc!
  "Set `key` in `conf` to cser/EDN representation of `val`."
  {:tag `Configuration}
  ([conf] conf)
  ([conf key val]
     (binding [*conf* conf]
       (assoc!* conf key val)))
  ([conf key val & kvs]
     (binding [*conf* conf]
       (apply assoc!* conf key val kvs))))

(defn ^:private edn-read-string
  "Like core `edn/read-string`, but using cser/EDN reader implementation."
  ([s] (edn-read-string {:eof nil, :readers *data-readers*} s))
  ([opts s] (when s (EdnReader/readString s opts))))

(defn get
  "Clojure data value for `key` in `conf`."
  ([conf key] (get conf key nil))
  ([conf key default]
     (binding [*conf* conf]
       (let [val (conf/get conf key nil)]
         (if (nil? val)
           default
           (edn-read-string val))))))
