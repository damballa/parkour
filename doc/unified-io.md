# Unified I/O

Parkour provides a collection-like interface for I/O against Hadoop-accessible,
record-oriented datasets (usually file-backed and on HDFS).  This interface is
unified across local and distributed processing, meaning that the objects
representing these datasets provide both local I/O and configuration for
MapReduce job I/O.

The base Hadoop MapReduce interfaces do not support incremental updates to
existing file-backed datasets.  Parkour reflects this fact by having two
distinct abstractions: distributed sequences for input and distributed sinks for
output.

## Distributed sequences (dseqs)

Parkour dseqs are the combination of two existing abstractions:

1. Parkour configuration step.  As a cstep, a particular dseq will configure a
   job for input from a particular Hadoop dataset.  The backing cstep (usually a
   function) implementing this configuration is the only thing particular to a
   “type” of dseq — the remaining dseq behavior is generic to all dseqs.
2. Clojure reduce-/fold-able.  Using the configuration provided by their cstep
   implementations, dseqs are able to drive instances of the Hadoop
   `InputFormat`s they specify to read locally from the configured input
   datasets.  Parkour dseqs provide access to this facility via the Clojure
   reducers `CollReduce` and `CollFold` protocols, allowing applications to
   locally `reduce` or (since 0.6.0) `fold` over remote Hadoop datasets exactly
   as if they were in-memory collections.

Job execution via the `parkour.graph` API yields dseqs for the results of the
executed job(s).  These dseqs may then be passed as input to subsequent jobs, or
`reduce`d locally to build in-memory values or final reports.

## Distributed sinks (dsinks)

Parkour distributed sinks are also a combination of two abstractions:

1. Parkour configuration step.  As a cstep, a particular dsink will configure a
   job for output to a particular Hadoop-accessible location, usually creating a
   new directory of files on HDFS.
2. Parkour dseq-able.  Parkour uses a protocol to turn other objects into dseqs.
   The default implementation assumes the object is a cstep configuring a Hadoop
   job for input, but dsinks provide a different implementation, instead
   offering a per-type “inverse” cstep which reads as input the output the dsink
   produces.

The `parkour.graph` API uses the latter abstraction to return input on the job
output as the result of execution.  This allows seamless chaining of jobs,
without redundantly specifying mirrored configurations for output then input.

### Transient output paths

Most dsink-generating functions for file-based output formats have two arities –
one which takes an explicit output path, and one which does not.  The arities
which do not will configure output to a generated transient output path on the
default filesystem.  Such paths will be deleted when leaving the current
resource scope (if one exists) or upon JVM exit.  These transient dsinks can be
very useful for intermediate outputs or outputs which are then processed locally
directly within the Parkour application.  If it is necessary to preserve output
written to a transient path, the `dseq/move!` function will move the resulting
dseq’s backing data to a specified path and return a new dseq on the new
location.
