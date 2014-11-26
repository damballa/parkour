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

## InputFormat

Hadoop `InputFormat` classes implement the translation from storage mechanisms
and formats to in-memory task input records.  The vast majority of even Parkour
MapReduce jobs will use existing Hadoop `InputFormat` implementations, but
Parkour does provide a mechanism allowing pairs of var-bound functions to stand
in for `InputFormat` classes.  Parkour invokes these pairs of functions
configuration-side then task-side respectively, the first to translate job
configuration to a collection of individual map-task configurations, then the
second to translate each map-task configuration to the actual task input
records.

Vars for these steps are bound to a class for Hadoop job configuration by the
`parkour.mapreduce/input-format!` function, along with vectors of optional
additional EDN-serializable parameters for each.  See the docstring of the
`input-format!` function for more details, and the source of the
[`parkour.io.range`][range] namespace for an example.

[range]: https://github.com/damballa/parkour/blob/master/src/clojure/parkour/io/range.clj

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

### Task environment

During task execution, Parkour configures several aspects of the dynamic
environment during the dynamic scope of each task:

- _Hadoop configuration_ – Parkour binds the default Hadoop configuration (as
  returned by the `conf/ig` function) to that of the running task.
- _Hadoop task context_ – Parkour binds the `parkour.mapreduce/*context*`
  dynamic var to the Hadoop task context of the active task.  This allows simple
  access to e.g. counters even in otherwise pure-functional task functions.
- _Resource scope_ – Parkour uses the [pjstadig/scopes][scopes] library to
  establish a resource scope for the extent of each task.  Task functions may
  depend on this in order to arrange for clean-up of acquired resources upon
  task completion.

[scopes]: https://github.com/pjstadig/scopes

### Adapters

For writing task functions, Parkour provides the following adapters and
associated interfaces.

#### mr/collfn (default)

The default task function adapter allows one to write functions which look, act,
and importantly _are_ exactly like standard Clojure collection functions.  With
this adapter, Parkour invokes the task function directly with any
configuration-serialized task arguments followed by the reducible/seqable input
collection.  The return value should be a reducible collection containing the
task output tuples.  Metadata on the var may specify a re-shaping of the task
input and output collections via the `::mr/source-as` and ``::mr/sink-as` keys
respectively.

```clj
(defn word-count-m
  {::mr/source-as :vals}
  [input]
  (->> input
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

### Task input

The task input parameter acts as a reducible collection over the task key-value
input tuples.  That collection may be directly reduced, or may be “reshaped”
into a reducible and seqable collection containing a particular “shape” of the
original key-value data.

Tasks may specify a particular input shape in one of two ways: by calling the
`mr/source-as` function with a shape keyword or function; or by using the
`collfn` adapter and specifying a shape keyword or function as the value of the
`::mr/source-as` key in their var metadata.

The following shape keywords are available in both map and reduce/combine tasks:

- `:default` – Use the default input shape specified in job configuration.  Will
  be `:keyvals` if unspecified, but dseqs for input formats with idiosyncratic
  key/value division may specify other shapes; e.g, `:vals` for
  `TextInputFormat`.
- `:keyvals` – Re-shape as vectors of key-vals pairs.  Nearly the identity
  transformation, but does allow the resulting collection to be seqable as well.
  This is the standard `Mapper` input shape.
- `:keys` – Just the keys from each key-value pair.
- `:vals` – Just the values from each key-value pair.

The following shape keywords are available only in reduce/combine tasks:

- `:keyvalgroups` – Vector pairs of the first instance of each distinct grouping
  key and a sub-collection of all the values associated with that grouping key.
  This is the standard `Reducer` input shape.
- `:keygroups` – Just the first instance of each distinct grouping key.
- `:valgroups` – Just the sub-collections of values associated with each
  distinct grouping key.
- `:keykeyvalgroups` – Vector pairs of the first instance of each distinct
  grouping key and a sub-collection all the vector pairs of specific keys and
  values associated with that grouping key.
- `:keykeygroups` – Vector pairs of the first instance of each distinct grouping
  key and a sub-collection all specific keys associated with that grouping key.
- `:keysgroups` – Just the sub-collections all specific keys associated with
  each distinct grouping key.

### Task output

Task outputs are delivered to a “sink” – usually (and usually implicitly) the
job context.  Many of the task adapter interfaces use the task function return
value as a collection to sink for task output.  These collections may be
metadata-annotated to indicate their “output shape.”  Task may specify the
output shape either by calling the `mr/sink-as` function on the result
collection, or by using the `collfn` adapter and specifying `::mr/sink-as` in
their var metadata.

The shape argument to `sink-as` may be a user-supplied function of two arguments
performing the actual sinking, or a keyword indicating a standard built-in
sinking function:

- `:default` – Use the default ouput shape specified in job configuration.  Will
  be `:keyvals` if unspecified, but dsinks for output formats with idiosyncratic
  key/value division may specify other shapes; e.g. `:keys` for
  `AvroKeyOutputFormat`.
- `:keyvals` – Sink collection as pairs of tuple keys and values.
- `:keys` – Sink collection values as output tuple keys, providing `nil` for
  each tuple value.
- `:vals` – Sink collection values as output tuple values, providing `nil` for
  each tuple key.
- `:none` – Consume input without actually sinking any tuples.

Parkour applies the `parkour.mapreduce/sink` function to these annotated
collections to write the output tuples.  One may also call it explicitly in
order to e.g. produce output from the reduction of a grouping sub-collection.

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
