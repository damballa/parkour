# Parkour REPL integration

Parkour includes several features explicitly designed to support REPL-based
development workflows.

## Cluster-connected REPL

The [lein-hadoop-cluster][lein-hadoop-cluster] Leiningen plugin simplifies
launching a development REPL configured to access a live Hadoop cluster.  When
used within such a REPL, all Parkour functionality will provide interactive
access to the live Hadoop environment.

### Hadoop Configuration

A plain cluster-connected REPL should work with nearly any functioning Hadoop
configuration.  For several of the following features to work correctly however,
you must ensure that certain standard Hadoop configuration parameters are *not*
marked as `<final>true</final>` in your configuration.  This does not mean these
parameters should be non-final in your production deployment configuration – in
fact, they almost certainly should be final there.  You merely need to ensure
they are non-final in your local development copy of the configuration.

The parameters are:

- `mapred.job.tracker` / `mapreduce.framework.name`
- `hadoop.tmp.dir`
- `mapred.local.dir`
- `mapred.system.dir`
- `mapreduce.jobtracker.staging.root.dir`
- `mapred.temp.dir`
- `fs.default.name` / `fs.defaultFS`
- `hadoop.security.authentication`

There may be other – PRs for additional parameters appreciated.

## Local-mode tests

The testing configuration support in `parkour.test-helpers` allows tests to run
in full local mode, even within a cluster-connected REPL process.  As long as
all tests use a test configuration (either explicitly or via the
`th/config-fixture` clojure.test fixture), then tests run within a
cluster-connected REPL will not interact with the live cluster.

## Mixed-mode experimentation

Hadoop jobs run in “mixed mode” execute tasks locally in the REPL process, but
with full access to the live cluster HDFS or other data sources.  This combines
the quick turn-around of local-mode execution with the ability to see the
results of jobs on real data.

The `parkour.conf/local-mr!` function will create or modify a configuration for
local execution, while preserving HDFS configuration.  Most datasets will be too
large to process locally in their entirety — after all, why otherwise would you
be processing them with Hadoop in the first place?  To solve that problem, the
`parkour.io.sample` namespace provides a wrapper dseq (and backing input format)
which allows jobs to run over only small random samples of their input data.

Together, local processing of small samples of real data supports the rapid
iteration which can make REPL-based development so effective.

```clj
(word-count (conf/local-mr!) "file:tmp/outpath/0"
            (sample/dseq (text/dseq "hdfs-giant-text-file.txt")))
```

## REPL-launched remote jobs

For projects using Leiningen, Parkour supports launching full remote jobs
directly from a cluster-connected REPL.

Leveraging [Alembic][alembic], Parkour can use an in-process (but
classpath-isolated) instance of Leiningen to build a job JAR and collect
dependencies.  If you are not already doing so, you must separately specify
`alembic` as a dependency in your `user` or `dev` profile (i.e., in your user
`profile.clj`).  With Alembic available, the `parkour.repl/launch!` function
allows execution of any function with a job JAR built and a Hadoop configuration
ready to deploy it.

```clj
(launch! {"mapred.reduce.tasks" 313} word-count "outpath/0"
         (text/dseq "hdfs-giant-text-file.txt"))
```

[lein-hadoop-cluster]: https://github.com/llasram/lein-hadoop-cluster
[alembic]: https://github.com/pallet/alembic
