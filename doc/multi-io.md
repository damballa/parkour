# Multiple I/O

It is sometimes useful for a MapReduce application to accept input from two
different sources in the same job, or to provide two different output datasets
from the same job.  Hadoop includes basic support for both (via the
[`MultipleInputs`][MultipleInputs] and [`MultipleOutputs`][MultipleOutputs]
classes respectively) which it is possible to use with Parkour via the Hadoop
Java API.  Alternatively, Parkour includes its own, more general multiple inputs
and multiple outputs support, integrated with the Parkour job graph API.

Parkour’s multiple I/O interfaces are built in terms of proxying input/output
formats which load whole variant job sub-configurations.  This design avoids the
need for special-purpose multiple I/O classes (e.g. `AvroMultipleOutputs`).  It
cannot handle variant sub-configurations for non-input/output format parts of
Hadoop (such as serialization), but such conflicts are rare.

## Multiple inputs

There are two possible meanings of “multiple inputs” in the context of Hadoop
MapReduce jobs.  The first means that the input for a MapReduce job comes from
the concatenation of multiple datasets, where each individual split comes
entirely from one dataset or the other.  This is the type of multiple input we
are concerned with here.  The second means that tasks (usually map tasks) read
simultaneously from multiple inputs e.g. for performing joins.  Parkour does not
as yet have any special support for this type of multiple input.

For the former or “multiplexing” type of multiple input, Parkour provides the
`parkour.io.mux` namespace and integration into the `parkour.graph` job graph
API.  This provides three ways of adding multiple inputs to a job:

- Explicit multiplexing dseq.  The `mux/dseq` function accepts any number of
  existing dseqs for any input formats and returns a new dseq combining them.
- Multiple inputs to a map step.  The `graph/map` function can accept a vector
  of input nodes for different dseqs and configures the job for multiplex input
  over their combination.
- Multiple mappers on multiple inputs.  The `graph/partition` function can
  accept a vector of map nodes, which can each specify a different mapper on a
  different set of inputs.  It configures the job for multiplex input and
  map-task dispatch on their aggregate combination.

Example:

```clj
(-> [(-> (pg/input dseq-a)
         (pg/map mapper-a))
     (-> [(pg/input dseq-b)
          (pg/input dseq-c)]
         (pg/map mapper-bc))]
    (pg/partition [Text LongWritable])
    ...)
```

## Multiple outputs

Writing multiple outputs from a MapReduce task can be resource intensive, but is
usually less resource intensive than running multiple jobs.  Multiple named
outputs with different semantic content are almost always a win over the
alternatives.  Simply segmenting output into separate files based on some
property of the output may not be worth the cost however, especially without
careful partitioning to limit the number of distinct outputs per task.
Additionally, the dseqs generated from dsinks written with segmented output will
not be correct, and currently require manual re-creation.

Parkour’s multiple outputs do however support both types of multiple output,
allowing all of the following in any combination within a particular job:

- Output to multiple named output configurations.
- Output to multiple task-determined files with a particular named output
  configuration.
- Output to any number of the above from both map and reduce tasks.

The `parkour.graph/output` function accepts either a single output dsink or an
even number of arguments forming name-dsink pairs.  The latter case triggers
multiple outputs configuration.  For example:

```clj
(-> ...
    (pg/output :output-a dsink-a
               :ouptut-b dsink-b)
    ...)
```

Task functions can then control output via the output-shaping functions in the
`parkour.io.dux` namespace.  These functions may be provided as the output shape
to `mr/sink-as` or as `::mr/sink-as` metadata on task function vars.  There are
two primary groups of multiple output shaping functions:

- `named-{keyvals,keys,vals}` – Write to the default output path for named
  outputs.  If called with an output name, returns a shaping function for
  writing to that specific output.  If used directly, expects output tuples to
  be pairs of the output name and actual output content.
- `prefix-{keyvals,keys,vals}` – Write to output paths determined by tuple
  content for named outputs.  If called with an output name, returns a shaping
  function for writing to that specific output, in which case expects output
  tuples to be pairs of file prefix and output content.  If used directly,
  expects output tuples to be triples of the output name, file prefix, and
  output content.

In map tasks, both functions will accept the special `::mr/map-output` output
name.  They will write such output tuples to the (reduce-bound) default task
output, ignoring the file path prefix in the case of the `prefix-`* functions.

### Example

In the Parkour [matrixify example][matrixify], the partition record offsets and
total counts are written as two separate outputs by the `dim-count-r` task
function.  These outputs are specified with the names `:data` and `:count` in
job configuration:

```clj
          (-> ...
              (pg/output :data (mra/dsink [:string index-value])
                         :counts (mra/dsink [:long :long])))
```

Then written to via the `dux/named-keyvals` output shaping function:

```clj
(defn dim-count-r
  "Perform a parallel count, indexing each key within the reduce task, emitting
data with parallel index (reducer, offset) tuple and final reducer count."
  {::mr/source-as :valgroups, ::mr/sink-as dux/named-keyvals}
  [coll]
  (let [red (conf/get-long mr/*context* "mapred.task.partition" -1)]
    (tr/mapcat-state (fn [i odims-vals]
                       [(inc i) (if (identical? ::finished odims-vals)
                                  [[:counts [red i]]]
                                  (map (fn [[odim val]]
                                         [:data [odim [[red i] val]]])
                                       odims-vals))])
                     0 (r/mapcat identity [coll [::finished]]))))
```

[MultipleInputs]: https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/lib/input/MultipleInputs.html
[MultipleOutputs]: https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/lib/output/MultipleOutputs.html
[matrixify]: https://github.com/damballa/parkour/blob/master/examples/parkour/example/matrixify.clj
