(ns parkour.cser
  (:refer-clojure :exclude [assoc! get read-string pr-str])
  (:require [clojure.core :as cc]
            [clojure.edn :as edn]
            [letterpress.core :as lp]
            [parkour.conf :as conf]
            [parkour.cser.readers :refer [data-readers]]
            [parkour.cser.printers :refer [data-printers]])
  (:import [java.io Writer]
           [clojure.lang RT]
           [org.apache.hadoop.conf Configuration]))

(def ^:internal ^:dynamic *conf*
  "Configuration of job being executed/configured during job configuration
de/serialization."
  nil)

(defmacro ^:private with-conf
  "Enter dynamic cser context of `conf` for `body` forms."
  [conf & body] `(binding [*conf* (conf/ig ~conf)] ~@body))

(defn ^:private read-string*
  "Like core `edn/read-string`, but using cser/EDN reader implementation."
  ([s] (read-string* {} s))
  ([opts s]
     (let [readers (merge *data-readers* data-readers (:readers opts))
           opts (assoc opts :readers readers)]
       (edn/read-string opts s))))

(defn read-string
  "Like core `edn/read-string`, but using cser/EDN reader implementation in the
cser context of `conf`."
  ([conf s] (with-conf conf (read-string* s)))
  ([conf opts s] (with-conf conf (read-string* opts s))))

(defn ^:private pr-str*
  "Like core `pr-str`, but with cser-specific printers."
  ([] "")
  ([x] (lp/pr-str {:printers data-printers} x))
  ([x & xs] (apply lp/pr-str {:printers data-printers} x xs)))

(defn pr-str
  "Like core `pr-str`, but in the cser context of `conf`."
  ([conf] "")
  ([conf x] (with-conf conf (pr-str* x)))
  ([conf x & xs] (with-conf conf (apply pr-str* x xs))))

(defn ^:private assoc!*
  "Internal implementation for `assoc!`."
  ([conf key val]
     (conf/assoc! conf key (pr-str* val)))
  ([conf key val & kvs]
     (let [conf (assoc!* conf key val)]
       (if (empty? kvs)
         conf
         (recur conf (first kvs) (second kvs) (nnext kvs))))))

(defn assoc!
  "Set `key` in `conf` to cser/EDN representation of `val`."
  {:tag `Configuration}
  ([conf] conf)
  ([conf key val] (with-conf conf (assoc!* conf key val)))
  ([conf key val & kvs] (with-conf conf (apply assoc!* conf key val kvs))))

(defn get
  "Clojure data value for `key` in `conf`."
  ([conf key] (get conf key nil))
  ([conf key default]
     (with-conf conf
       (let [val (conf/get conf key nil)]
         (if (nil? val)
           default
           (read-string* val))))))
