(ns parkour.wrapper
  (:require [clojure.reflect :as reflect]
            [parkour (conf :as conf)]
            [parkour.util :refer [returning]])
  (:import [clojure.lang IPersistentVector]
           [org.apache.hadoop.io
             NullWritable Text Writable IntWritable LongWritable
             DoubleWritable FloatWritable]
           [org.apache.hadoop.util ReflectionUtils]))

(defprotocol Wrapper
  "Protocol for working with mutable wrapper objects, such as Hadoop
Writables."
  (unwrap [wobj] "Unwrap `wobj` in a type-specific fashion.")
  (rewrap [wobj obj] "Mutate wapper `wobj` to wrap `obj`."))

(defn unwrap-all
  "Unwrap all members of `coll` (usually key/value) to new vector."
  [coll] (mapv unwrap coll))

(defn ^:private getter?
  "True iff method `m` a getter method."
  [m] (and (= 'get (:name m)) (->> m :parameter-types count zero?)))

(defn ^:private setter?
  "True iff method `m` a setter method."
  [m] (and (= 'set (:name m)) (->> m :parameter-types count (= 1))))

(def ^:private prim->obj
  "Map from primitive type symbols to reference type symbols."
  {'byte `Byte,
   'short `Short, 'char `Character,
   'int `Integer, 'float `Float,
   'long `Long, 'double `Double})

(defn ^:private test+setter
  [s-wobj m]
  (let [t (-> m :parameter-types first), t' (prim->obj t t)]
    `[(instance? ~t') (.set ~s-wobj ~(vary-meta 'obj assoc :tag t))]))

(defn ^:private auto-wrapper*
  "Return forms for auto-generated `Wrapper` implementation for class `c`."
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
  "Auto-generate `Wrapper` implementations for classes `cs`."
  [& cs] `(do ~@(map auto-wrapper* cs)))

(extend-protocol Wrapper
  NullWritable
  (unwrap [wobj] nil)
  (rewrap [wobj obj] wobj)

  nil
  (unwrap [wobj] nil)
  (rewrap [wobj obj] nil)

  Text
  (unwrap [wobj] (.toString wobj))
  (rewrap [wobj obj]
    (returning wobj (.set wobj ^String obj)))

  Object
  (unwrap [wobj] wobj))

(auto-wrapper
  IntWritable
  LongWritable
  DoubleWritable
  FloatWritable)

(defmulti new-instance*
  (fn dispatch
    ([klass] klass)
    ([conf klass] klass)))

(defn ^:private ->class
  [x] (if (class? x) x (class x)))

(defn new-instance
  "Return a new instance of the class of `klass`, or of `klass` itself
if a class.  If Hadoop `conf` is provided and `klass` is Configurable,
configure the new instance with `conf`."
  ([klass] (new-instance* (->class klass)))
  ([conf klass] (new-instance* (conf/ig conf) (->class klass))))

(defmethod new-instance* nil
  ([klass] nil)
  ([conf klass] nil))

(defmethod new-instance* :default
  ([klass] (.newInstance ^Class klass))
  ([conf klass] (ReflectionUtils/newInstance ^Class klass ^Configuration conf)))

(defmethod new-instance* NullWritable
  ([_] (NullWritable/get))
  ([_ _] (NullWritable/get)))

(defn clone
  "Return a clone of wrapper object `wobj`."
  ([wobj] (rewrap (new-instance wobj) (unwrap wobj)))
  ([conf wobj] (rewrap (new-instance conf wobj) (unwrap wobj))))

(defn ^:private wrap-keyvals*
  [f k v]
  (let [[k v :as tuple] (mapv f [k v])]
    (fn [[k' v']]
      (returning tuple
        (rewrap k k')
        (rewrap v v')))))

(defn wrap-keyvals
  "Return a function which wraps its key/value pair argument in
instances of the wrapper type `t` or types `k` & `v`."
  ([k v] (wrap-keyvals* new-instance k v))
  ([conf k v] (wrap-keyvals* (partial new-instance conf) k v)))

(defn wrap-vals
  "Return a function which wraps its argument in an instance of the
wrapper type `t`."
  ([t] (partial rewrap (new-instance t)))
  ([conf t] (partial rewrap (new-instance conf t))))
