(ns parkour.wrapper
  (:require [clojure.reflect :as reflect]
            [parkour.util :refer [returning]])
  (:import [clojure.lang IPersistentVector]
           [org.apache.hadoop.io
             NullWritable Text Writable IntWritable LongWritable
             DoubleWritable FloatWritable]))

(defprotocol Wrapper
  (unwrap [wobj] "Unwrap `wobj` in a type-specific fashion.")
  (rewrap [wobj obj] "Mutate wapper `wobj` to wrap `obj`."))

(defn ^:private getter?
  [m] (and (= 'get (:name m)) (->> m :parameter-types count zero?)))

(defn ^:private setter?
  [m] (and (= 'set (:name m)) (->> m :parameter-types count (= 1))))

(def ^:private prim->obj
  {'byte `Byte,
   'short `Short, 'char `Character,
   'int `Integer, 'float `Float,
   'long `Long, 'double `Double})

(defn ^:private test+setter
  [s-wobj m]
  (let [t (-> m :parameter-types first), t' (prim->obj t t)]
    `[(instance? ~t') (.set ~s-wobj ~(vary-meta 'obj assoc :tag t))]))

(defn ^:private auto-wrapper*
  [c]
  (let [^Class c (ns-resolve *ns* c), cname (symbol (.getName c))
        members (:members (reflect/reflect c))
        getter (first (filter getter? members))
        setters (filter setter? members)
        s-wobj (vary-meta 'wobj assoc :tag cname)]
    (when-not (and getter (seq setters))
      (throw (ex-info (format "%s: auto-wrapper failed" (str c))
                      {:class c, :getter getter, :setters setters})))
    `(extend-protocol Wrapper
       ~c
       (~'unwrap [~s-wobj] (.get ~s-wobj))
       (~'rewrap [~s-wobj ~'obj]
         (returning ~s-wobj
           ~(if (= 1 (count setters))
              `(.set ~s-wobj ~'obj)
              `(cond ~@(mapcat (partial test+setter s-wobj) setters))))))))

(defmacro auto-wrapper
  [& cs] `(do ~@(map auto-wrapper* cs)))

(extend-protocol Wrapper
  NullWritable
  (unwrap [wobj] nil)
  (rewrap [wobj obj] wobj)

  Text
  (unwrap [wobj] (.toString wobj))
  (rewrap [wobj obj]
    (returning wobj (.set wobj ^String obj)))

  Object
  (unwrap [wobj] wobj)
  (rewrap [wobj obj]
    (throw (ex-info "rewrap not implemented" {:wobj wobj, :obj obj}))))

(auto-wrapper
  IntWritable
  LongWritable
  DoubleWritable
  FloatWritable)

(defn ^:private new-instance
  [c]
  (let [^Class c (if (class? c) c (class c))]
    (.newInstance c)))

(defn clone
  [wobj] (rewrap (new-instance wobj) (unwrap wobj)))

(defn wrap-keyvals
  ([t] (wrap-keyvals t t))
  ([k v]
     (let [[k v :as tuple] (mapv new-instance [k v])]
       (fn [[k' v']]
         (returning tuple
           (rewrap k k')
           (rewrap v v'))))))

(defn wrap-vals
  [t] (partial rewrap (new-instance t)))
