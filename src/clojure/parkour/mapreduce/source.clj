(ns parkour.mapreduce.source
  (:refer-clojure :exclude [key val keys vals])
  (:require [clojure.core :as cc]
            [clojure.core.reducers :as r]
            [clojure.core.protocols :as ccp]
            [parkour (conf :as conf) (cser :as cser) (wrapper :as w)
             ,       (reducers :as pr)]
            [parkour.util :refer [returning mev]]
            [parkour.util.map-combine :refer [map-combine]])
  (:import [clojure.lang Seqable IteratorSeq]
           [java.io Closeable]
           [java.util Collection]
           [org.apache.hadoop.conf Configurable]
           [org.apache.hadoop.mapreduce MapContext ReduceContext]
           [org.apache.hadoop.mapreduce TaskInputOutputContext]))

(defprotocol TupleSource
  "Internal protocol for iterating over key/value tuples from a source
of such tuples."
  (key [source]
    "Current tuple's key.")
  (val [source]
    "Current tuple's value.")
  (vals [source]
    "Current key's sequence of associated values.")
  (next-keyval [source]
    "Source updated to next key/value tuple, implementation.")
  (next-key [source]
    "Source updated to next distinct key, implementation.")
  (-initialize [source]
    "Initialize the source for reading tuples.")
  (-close [source]
    "Close the source, cleaning up any associated resources.")
  (-nsplits [source]
    "Number of splits into which source may be divided.")
  (-splits [source]
    "Sequence of tuple sources for each split of original source."))

(declare source-as)

(defn source?
  "True iff `x` is a tuple source."
  [x] (satisfies? TupleSource x))

(defn initialize
  "Initialize the source for reading tuples."
  {:tag `Closeable}
  [source] (returning source (-initialize source)))

(defn nsplits
  "Number of splits into which source may be divided."
  ^long [source] (-nsplits source))

(defn splits
  "Sequence of tuple sources for each split of original source."
  [source] (case (nsplits source) 0 nil, 1 [source], #_else (-splits source)))

(defn keyval
  "Pair of current tuple's key and value."
  [context] (mev (key context) (val context)))

(defn keyvals
  "Pair of current tuple's key and sequence of associated values."
  [context] (mev (key context) (vals context)))

(defn mapping
  "A `seq`able or `reduce`able mapping of `f` for each item in `coll`."
  [f coll]
  (reify
    ccp/CollReduce
    (coll-reduce [this f1] (ccp/coll-reduce this f1 (f1)))
    (coll-reduce [_ f1 init] (r/reduce f1 init (r/map f coll)))

    r/CollFold
    (coll-fold [_ n combinef reducef]
      (r/fold n combinef reducef (r/map f coll)))

    Seqable
    (seq [_] (map f coll))))

(defn keykeyvals
  "Pair of current tuple's key and sequence of grouped key and value pairs."
  [context] [(key context) (mapping #(mev (key context) %) (vals context))])

(defn keykeys
  "Pair of current tuple's key and sequence of grouped keys."
  [context] [(key context) (mapping (fn [_] (key context)) (vals context))])

(defn keys
  "Current grouping key's sequence of grouped keys."
  [context] (mapping (fn [_] (key context)) (vals context)))

(defn source-reduce*
  "Single-source implementation of `source-reduce`."
  [nextf dataf source f init]
  (with-open [source (initialize source)]
    (loop [acc init]
      (if-not (nextf source)
        acc
        (let [acc (f acc (dataf source))]
          (if (reduced? acc)
            acc
            (recur acc)))))))

(defn source-reduce
  "As per `reduce`, but in terms of the `TupleSource` protocol.  When provided,
applies `nextf` to `source` to retrieve the next tuple source for each iteration
and `dataf` to retrieve the tuple values passed to `f`."
  ([source f init]
     (source-reduce next-keyval keyval source f init))
  ([nextf dataf source f init]
     (case (nsplits source)
       0 init
       1 (let [acc (source-reduce* nextf dataf source f init)]
           (if (reduced? acc) @acc acc))
       , (reduce (fn [acc source]
                   (source-reduce* nextf dataf source f acc))
                 init (splits source)))))

(defn source-fold
  "As per `r/fold`, but in terms of the `TupleSource` protocol."
  ([source combinef reducef]
     (source-fold next-keyval keyval source combinef reducef))
  ([nextf dataf source combinef reducef]
     (case (nsplits source)
       0 (combinef)
       1 (source-reduce* nextf dataf source reducef (combinef))
       , (let [mapf #(source-reduce* nextf dataf % reducef (combinef))]
           (map-combine mapf combinef (splits source))))))

(defn source-seq*
  "Single-source implementation of `source-seq`."
  [nextf dataf source]
  ((fn step [source]
     (lazy-seq
      (if-not (nextf source)
        (-close source)
        (cons (dataf source) (step source)))))
   (initialize source)))

(defn source-seq
  "As per `seq`, but in terms of the `TupleSource` protocol."
  ([source]
     (source-seq next-keyval keyval source))
  ([nextf dataf source]
     (case (nsplits source)
       0 nil
       1 (source-seq* nextf dataf source)
       , (mapcat (partial source-seq* nextf dataf) (splits source)))))

(defn reducer
  "Make a tuple source `source` `reduce`able and `seq`able with particular
iteration function `nextf` and extraction function `dataf`."
  [nextf dataf source]
  (let [reducer (partial reducer nextf dataf)]
    (reify
      ccp/CollReduce
      (coll-reduce [this f] (ccp/coll-reduce this f (f)))
      (coll-reduce [_ f init] (source-reduce nextf dataf source f init))

      r/CollFold
      (coll-fold [_ _ combinef reducef]
        (source-fold nextf dataf source combinef reducef))

      Seqable
      (seq [_] (source-seq nextf dataf source))

      Closeable
      (close [_] (-close source))

      TupleSource
      (key [_] (key source))
      (val [_] (val source))
      (vals [_] (vals source))
      (next-keyval [this] (next-keyval source))
      (next-key [this] (next-key source))
      (-initialize [_] (-initialize source))
      (-close [_] (-close source))
      (-nsplits [_] (-nsplits source))
      (-splits [_] (map reducer (-splits source))))))

(defn seq-source
  "Make a tuple source from `seq`able collection `coll`."
  [coll]
  (let [coll (seq coll), tuples (atom coll)]
    (reify
      TupleSource
      (key [_] (nth (first @tuples) 0))
      (val [_] (nth (first @tuples) 1))
      (next-keyval [_] (boolean (swap! tuples next)))
      (-initialize [_])
      (-close [_])
      (-nsplits [_] 1)
      (-splits [this] [this])

      Closeable
      (close [_])

      Seqable
      (seq [_] coll))))

(extend-protocol TupleSource
  nil
  (key [_] nil)
  (val [_] nil)
  (next-keyval [_] false)
  (-initialize [_])
  (-close [_])
  (-nsplits [_] 0)
  (-splits [_] nil)

  MapContext
  (key [this] (.getCurrentKey this))
  (val [this] (.getCurrentValue this))
  (next-keyval [this] (.nextKeyValue this))
  (-initialize [_])
  (-close [_])
  (-nsplits [_] 1)
  (-splits [this] [this])

  ReduceContext
  (key [this] (.getCurrentKey this))
  (val [this] (.getCurrentValue this))
  (vals [this]
    (let [vals (.getValues this)]
      (reify
        Seqable
        (seq [_] (-> vals .iterator IteratorSeq/create))

        ccp/CollReduce
        (coll-reduce [this f] (ccp/coll-reduce this f (f)))
        (coll-reduce [_ f init] (r/reduce f init vals))

        r/CollFold
        (coll-fold [_ n combinef reducef]
          (r/fold n combinef reducef vals)))))
  (next-keyval [this] (.nextKeyValue this))
  (next-key [this] (.nextKey this))
  (-initialize [_])
  (-close [_])
  (-nsplits [_] 1)
  (-splits [this] [this]))

(extend-type TaskInputOutputContext
  ccp/CollReduce
  (coll-reduce
    ([this f] (ccp/coll-reduce this f (f)))
    ([this f init] (source-reduce this f init)))

  r/CollFold
  (coll-fold [this _ combinef reducef]
    (source-fold this combinef reducef)))

(defn empty-source
  "Concrete (non-`nil`) tuple source with no tuples."
  [conf]
  (let [conf (conf/ig conf)]
    (reify
      Configurable
      (getConf [_] conf)

      TupleSource
      (next-keyval [this] false)
      (-initialize [_])
      (-close [_])
      (-nsplits [_] 0)
      (-splits [this] nil)

      Closeable
      (close [this])

      ccp/CollReduce
      (coll-reduce [this f] (f))
      (coll-reduce [this f init] init)

      r/CollFold
      (coll-fold [this _ combinef reducef] (combinef))

      Seqable
      (seq [this] nil))))

(defn unwrap-source
  "Produce \"unwrapper\" for `source`, which unwraps each accessed entry."
  [source]
  (reify
    Configurable
    (getConf [_] (conf/ig source))

    TupleSource
    (key [_] (w/unwrap (key source)))
    (val [_] (w/unwrap (val source)))
    (vals [_] (mapping w/unwrap (vals source)))
    (next-keyval [this] (next-keyval source))
    (next-key [this] (next-key source))
    (-initialize [_] (-initialize source))
    (-close [_] (-close source))
    (-nsplits [_] (-nsplits source))
    (-splits [this] (map unwrap-source (-splits source)))

    Closeable
    (close [_] (-close source))

    ccp/CollReduce
    (coll-reduce [this f] (ccp/coll-reduce this f (f)))
    (coll-reduce [this f init] (source-reduce this f init))

    r/CollFold
    (coll-fold [this _ combinef reducef]
      (source-fold this combinef reducef))

    Seqable
    (seq [this] (source-seq this))))

(extend-protocol w/Wrapper
  TaskInputOutputContext
  (unwrap [wobj] (unwrap-source wobj)))

(defn shape-default
  "Produce default source shape tuples from `context`."
  [context]
  (if-not (= "map" (conf/get context "parkour.step" "map"))
    context
    (let [shape (cser/get context "parkour.source-as.default" identity)]
      (source-as shape context))))

(defn shape-keys
  "Produce keys only from the tuples in `context`."
  [context]
  (case (-> context meta :parkour.mapreduce.sink/sink-as)
    :keys context
    :vals (mapping (constantly nil) context)
    #_else (if-not (source? context)
             (mapping #(nth % 0) context)
             (reducer next-keyval key context))))

(defn shape-vals
  "Produce values only from the tuples in `context`."
  [context]
  (case (-> context meta :parkour.mapreduce.sink/sink-as)
    :keys (mapping (constantly nil) context)
    :vals context
    #_else (if-not (source? context)
             (mapping #(nth % 1) context)
             (reducer next-keyval val context))))

(defn shape-keyvals
  "Produce pairs of keys and values from the tuples in `context`."
  [context]
  (case (-> context meta :parkour.mapreduce.sink/sink-as)
    :keys (mapping #(mev % nil) context)
    :vals (mapping #(mev nil %) context)
    #_else (if-not (source? context)
             context
             (reducer next-keyval keyval context))))

(defn shape-keygroups
  "Produce distinct keys from the tuples in `context`."
  [context] (reducer next-key key context))

(defn shape-valgroups
  "Produce sequences of values associated with distinct grouping keys from the
tuples in `context`."
  [context] (reducer next-key vals context))

(defn shape-keyvalgroups
  "Produce pairs of distinct group keys and associated sequences of values from
the tuples in `context`."
  [context] (reducer next-key keyvals context))

(defn shape-keykeyvalgroups
  "Produce pairs of distinct grouping keys and associated sequences of specific
keys and values from the tuples in `context`."
  [context] (reducer next-key keykeyvals context))

(defn shape-keykeygroups
  "Produce pairs of distinct grouping keys and associated sequences of specific
keys from the tuples in `context`."
  [context] (reducer next-key keykeys context))

(defn shape-keysgroups
  "Produce sequences of specific keys associated with distinct grouping keys
from the tuples in `context`."
  [context] (reducer next-key keys context))

(def source-fns
  "Map of keywords to built-in source-shaping functions."
  {:default shape-default
   :keys shape-keys
   :vals shape-vals
   :keyvals shape-keyvals
   :keygroups shape-keygroups
   :valgroups shape-valgroups
   :keyvalgroups shape-keyvalgroups
   :keykeyvalgroups shape-keykeyvalgroups
   :keykeygroups shape-keykeygroups
   :keysgroups shape-keysgroups
   })

(defn source-fn
  "Source-shaping function for keyword or function `f`."
  [f]
  (doto (get source-fns f f)
    (as-> f (when (keyword? f)
              (throw (ex-info (str "Unknown built-in source `:" f "`")
                              {:f f}))))))

(defn source-as
  "Shape `source` to the collection shape `kind`."
  [kind source] ((source-fn kind) source))
