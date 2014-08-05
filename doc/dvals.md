# Distributed values

Parkour distributed values (dvals) provide a value-oriented interface for using
the Hadoop distributed cache in Parkour MapReduce applications.

## Overview

The Hadoop distributed cache provides data locality for “small” secondary input
files (usually <<1GB) across all tasks of a MapReduce job.  Very small inputs
(<1K) may ship directly embedded in the job configuration, but the distributed
cache is a better fit for most data files.  Leveraging the distributed cache can
improve both job simplicity and performance by avoiding costly alternatives such
as reduce-side joins.

Unfortunately, the native Hadoop interfaces for accessing the distributed cache
are particularly unwieldy.  Cache entries are set as part of job configuration,
then made available at local file paths during task execution.  This mechanism
simultaneously couples configuration and task functions while obscuring the
origin of cache-delivered datasets.  Most critically, these cached file are
placed (via symlinks) directly into the working directory of task processes
during on-cluster execution, but _not_ during local-mode execution.  This
divergence makes it much more difficult to test (and debug!) tasks using the
distributed cache.

In order to work around these issue, Parkour provides an alternative,
value-oriented interface to the distributed cache in the form of distributed
values, or dvals.  Using dvals allows your code to decouple the mechanics of
loading secondary datafiles from the logic of using the resulting values.

## API

The Parkour dval API consists of three rough levels, all within the
`parkour.io.dval` namespace: distributed-cache–able paths, base distributed
values, and higher-level helpers.

### dcpaths

Distributed-cache–able paths (or dcpaths) are path values (Hadoop `Path`
subclass instances) which explicitly indicate files which should be added to the
distributed cache.  When dcpaths are passed as arguments to task function-vars
via job configuration, they participate in an interface which allows them to
make arbitrary modifications to the job configuration they are being serialized
into.  Using this interface, dcpaths add themselves to the distributed cache,
then arrange to deserialize task-side as the path of the distributed-cached copy
of the original file.

The `parkour.io.dval/dcpath` function creates dcpaths from normal paths.  This
allows passing paths to task functions via the distributed cache, which can be
useful in some circumstances.  Generally however it is more convenient to use
the value-oriented API provided by dvals.

### Base dvals

Parkour dvals are Clojure reference types similar to delays, but which capture a
function-var plus arguments as explicitly separate and EDN-serializable values.
These component values form an executable and serializable “recipe” for the
dval’s value, allowing complete and compact serialization of the dval even if
its computed value supports neither.  Dvals may be passed as arguments to
MapReduce tasks, in which case they deserialize task-side as delays over
evaluation of their recipes.

The `parkour.io.dval` namespace provides two base functions for creating dvals:

- `dval` – Creates a dval which acts as a delay over applying a function-var to
  any number of additional arguments.
- `value-dval` – Like `dval`, but takes an additional initial argument as the
  already-computed local value of the dval.

### Higher-level helpers

The real usefulness of dvals emerges when they are combined with dcpaths,
providing serializable recipes for computing values from artifacts in the
distributed cache.

The `parkour.io.dval` namespace provides a number of functions for creating
dvals over loading values from files which should ship to tasks via the
distributed cache:

- `load-dval` – Accepts a data-loading function-var, optional list of non-source
  parameters, and a list of data source paths.  Converts the sources to dcpaths
  for shipment via the distributed cache and reconstitutes as application of the
  function-var to the concatenation of all parameters.
- `copy-dval` – Like `load-dval`, but the source files are first copied to
  transient paths on the default (HDFS) filesystem.  Useful for distributing
  values read from local files or files accessed via HTTP.
- `transient-dval` – Accepts a data-writing function, data-loading var, and
  existing value.  Uses the writing function to write the value to a transient
  path, which is then distributed.

The `transient-dval` function serves as a base for a number of functions which
support shipping existing values by a variety of serialization mechanisms:

- `parkour.io.dval/edn-dval` – Yield dval on EDN serialization of value.
- `parkour.io.dval/jser-dval` – Yield dval on Java serialization of value.
- `parkour.io.avro/dval` – Yield dval on Avro serialization of value.

### Utilization

The content of a dval may be accessed via `deref`/`@`, just as with any other
reference type, and may be accessed both locally and within task functions.  The
first such access within a process will cause the dval’s value to be computed
(if necessary) by executing the dval’s recipe, but the result will be stored for
subsequent accesses.

Dvals may be passed to MapReduce tasks like any other arguments, and any dcpath
parameters will deliver their referenced content via the distributed cache as
described above.

## Example

In the Parkour [matrixify example][matrixify], the row/column offset indices are
calculated locally and wrapped in dvals:

```clj
  (let [...
        c-offsets (dval/edn-dval (offsets c-counts))
        r-offsets (dval/edn-dval (offsets r-counts))]
    ...)
```

The offset map dvals are passed to the final job map task:

```clj
    (-> (pg/input ...)
        (pg/map #'absind-m c-offsets r-offsets)
        ...)
```

Which then dereferences them to access and use the content:

```clj
  (let [c-offsets @c-offsets, r-offsets @r-offsets]
    (r/map (fn [[col [row val]]]
             [(absind r-offsets row) (absind c-offsets col) val])
           ...))
```

[matrixify]: https://github.com/damballa/parkour/blob/master/examples/parkour/example/matrixify.clj
