# Parkour MapReduce in depth

Parkour tries to integrate Clojure’s and Hadoop’s abstractions in a
straightforward fashion, but not all of its interfaces naively mirror the
associated Java Hadoop interfaces.

For many of the interfaces where Hadoop invokes a user-provided var via Parkour,
Parkour makes use of “adapter functions” to adapt that user-provided var to the
interface.  The lowest level integration in Parkour will closely follow the
underlying interface, but the per-interface default adapter function will allow
users to provide a more Clojure-idiomatic function.  The user may provide their
own adapter function to use instead, including e.g. `identity` in order to
directly implement that lowest-level interface.  The desired adapter is
specified as `:parkour.mapreduce/adapter` metadata on user-provided vars.

Each of the Hadoop-invoked entry points Parkour exposes has its own specific
interface and set of built-in adapters, which are described below.

## Mapper and Reducer

In Parkour, the var-bound functions standing in for `Mapper` and `Reducer`
classes share exactly the same interface.  These functions implement the primary
activity of a MapReduce task, consuming the task’s input tuples and writing the
task’s output tuples.

Task vars for mappers, reducers, and combine-stage reducers are bound to classes
by respectively the `parkour.mapreduce/{mapper!,reducer!,combiner!}` functions.
Each of these functions also accepts any number of additional configuration-time
arguments which Parkour serializes into the job configuration as EDN and
automatically deserializes and provides to the task var during task execution.

Tasks may be added to job nodes via the `parkour.graph/{map,reduce,combine}`
functions, which work as their analogs above.

### Adapters

For writing task functions, Parkour provides the following adapters and
associated interfaces.

#### mr/collfn (default)

The default task function adapter allows one to write functions which look, act,
and importantly _are_ exactly like standard Clojure collection functions.  With
this adapter, Parkour invokes the task function directly with any
configuration-serialized task arguments followed by the reducible/seqable input
collection.  The return value should be a reducible collection containing the
task output tuples.

```clj
(defn word-count-m
  [input]
  (->> (mr/vals input)
       (r/mapcat #(str/split % #"\s+"))
       (r/map #(-> [% 1]))))
```

#### identity

The base interface provides direct access to the job configuration, job context
object, raw Hadoop serialization wrappers, and produces the actual task function
via a higher-order function.  The user-provided task var is invoked with the job
configuration followed by any configuration-serialized arguments; it should
return a function of one argument.  That function is then invoked with the job
context to perform task execution, and its return value is ignored.

```clj
(defn word-count-m
  {::mr/adapter identity}
  [conf]
  (fn [context]
    (->> context w/unwrap mr/vals
         (r/mapcat #(str/split % #"\s+"))
         (r/map #(-> [% 1]))
         (mr/sink (mr/wrap-sink context)))))
```

#### mr/contextfn

The `contextfn` adapter provides an intermediate step between the raw interface
and the `collfn` interface.  The task input and output are automatically
unwrapped and wrapped and the function return value is used as output.  But the
task function still has direct access to the job configuration and context, and
is produced via a higher-order function.  With this adapter, the task var is
invoked with the job configuration followed by configuration-serialized
arguments; it should return a function of two arguments.  That function is then
invoked with the job context and the collection of unwrapped input tuples; its
return value should be a reducible collection, which is used as the task output.

```clj
(defn word-count-m
  {::mr/adapter mr/contextfn}
  [conf]
  (fn [context input]
    (->> (mr/vals input)
         (r/mapcat #(str/split % #"\s+"))
         (r/map #(-> [% 1])))))
```

### Input sources

The task input parameter acts as a reducible collection over the task key-value
input tuples.  That collection may be directly reduced, or “reshaped” into a
reducible and seqable collection containing a particular “shape” of the original
key-value data.  The following reshaping functions are available in both map and
reduce/combine tasks:

- `parkour.mapreduce/keyvals` – “Re-shape” as vectors of key-vals pairs.  Nearly
  the identity transformation, but does allow the resulting collection to be
  seqable as well.  This is the standard `Mapper` input shape.
- `parkour.mapreduce/keys` – Just the keys from each key-value pair.
- `parkour.mapreduce/vals` – Just the values from each key-value pair.

The following reshaping functions are available only in reduce/combine tasks:

- `parkour.mapreduce/keyvalgroups` – Vector pairs of the first instance of each
  distinct grouping key and a sub-collection of all the values associated with
  that grouping key.  This is the standard `Reducer` input shape.
- `parkour.mapreduce/keygroups` – Just the first instance of each distinct
  grouping key.
- `parkour.mapreduce/valgroups` – Just the sub-collections of values associated
  with each distinct grouping key.
- `parkour.mapreduce/keykeyvalgroups` – Vector pairs of the first instance of
  each distinct grouping key and a sub-collection all the vector pairs of
  specific keys and values associated with that grouping key.
- `parkour.mapreduce/keykeygroups` – Vector pairs of the first instance of each
  distinct grouping key and a sub-collection all specific keys associated with
  that grouping key.
- `parkour.mapreduce/keysgroups` – Just the sub-collections all specific keys
  associated with each distinct grouping key.

### Output sinks

Task outputs are delivered to a “sink” – usually (and usually implicitly) the
job context.  Many of the task adapter interfaces use the task function return
value as a collection to sink for task output.  These collections may be
metadata-annotated to indicate their “output shape” using the
`parkour.mapreduce/sink-as` function.  The shape argument to `sink-as` may be a
user-supplied function of two arguments performing the actual sinking, or a
keyword indicating a standard built-in sinking function:

- `:keyvals` – Sink collection as pairs of tuple keys and values.
- `:keys` – Sink collection values as output tuple keys, providing `nil` for
  each tuple value.
- `:vals` – Sink collection values as output tuple values, providing `nil` for
  each tuple key.
- `:none` – Consume input without actually sinking any tuples.

Parkour applies the `parkour.mapreduce/sink` function to these annotated
collection to write the output tuples.  One may also call it explicitly in order
to e.g. produce output from the reduction of a grouping sub-collection.

The output shape annotations also allow composition of task functions for the
above shapes – `(mr/keys (mr/sink-as :keys coll))` is a no-op, and `(mr/keys
(mr/sink-as :keyvals coll))` is (almost) the same as `(map first coll)`.

## Partitioner

Parkour partitioner functions follow essentially the same interface as the
`Partitioner#getPartition()` method, with handful of adapter variations.  A
partitioner var is bound to the Parkour partitioner class with the
`parkour.mapreduce/partitioner!` function.  A partitioner var is added to a job
node with the `parkour.graph/partition` function.  Both functions also accept
any number of additional var arguments to serialize via the job configuration.

### Adapters

Parkour provides the following partitioner function adapters.

#### (comp mr/partfn constantly) (default)

The default partitioner adapter allows writing unparameterized partitioner
functions which act on unwrapped tuple keys and values.  The function will be
called with three arguments: the unwrapped key, the unwrapped value, and an
integral count of the number of reducers; it should return an integral value mod
the reducer count, and should be primitive type-hinted as `OOLL`.

```clj
(defn first-letter-p
  ^long [key val ^long nparts]
  (-> ^String key (.charAt 0) int (mod nparts)))
```

#### identity

The raw interface is a higher-order function interface, where the partitioner
var function is invoked with the job configuration plus any
configuration-serialized parameters and should return an `OOLL`-hinted function.
That function will be invoked with the raw Hadoop serialization wrappers for the
key and value, but is otherwise as above.

```clj
(defn first-letter-p
  {::mr/adapter identity}
  [conf]
  (fn ^long [key val ^long nparts]
    (-> key str (.charAt 0) int (mod nparts))))
```

#### mr/partfn

A compromise between the default and raw interfaces.  With this adapter, the
provided var is invoked as a higher-order function as in the raw interface, but
the returned function will itself be called with the unwrapped key and value.

```clj
(defn first-letter-p
  {::mr/adapter mr/partfn}
  [conf]
  (fn ^long [key val ^long nparts]
    (-> ^String key (.charAt 0) int (mod nparts))))
```
