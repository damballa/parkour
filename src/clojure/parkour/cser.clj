(ns parkour.cser
  (:refer-clojure :exclude [assoc! get read-string pr-str])
  (:require [clojure.core :as cc]
            [parkour.conf :as conf])
  (:import [java.io Writer]
           [clojure.lang RT]
           [org.apache.hadoop.conf Configuration]
           [parkour.edn EdnReader]))

(def ^:internal ^:dynamic *conf*
  "Configuration of job being executed/configured during job configuration
de/serialization."
  nil)

(defmacro ^:private with-conf
  "Enter dynamic cser context of `conf` for `body` forms."
  [conf & body] `(binding [*conf* (conf/ig ~conf)] ~@body))

(defn ^:private read-string*
  "Like core `edn/read-string`, but using cser/EDN reader implementation."
  ([s] (read-string* {:eof nil, :readers *data-readers*} s))
  ([opts s] (when s (EdnReader/readString s opts))))

(defn read-string
  "Like core `edn/read-string`, but using cser/EDN reader implementation in the
cser context of `conf`."
  ([conf s] (with-conf conf (read-string* s)))
  ([conf opts s] (with-conf conf (read-string* opts s))))

(defn pr-str
  "Like core `pr-str`, but in the cser context of `conf`."
  [conf & xs] (with-conf conf (apply cc/pr-str xs)))

(defn ^:private assoc!*
  "Internal implementation for `assoc!`."
  ([conf key val]
     (conf/assoc! conf key (cc/pr-str val)))
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
