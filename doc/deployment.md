# Parkour Deployment

Hadoop job deployment requires slightly more work than general JVM application
deployment.  In addition to base deployment issues, Hadoop applications must
also ensure that they allow for user configuration of cluster-/data-dependent
parameters, include the correct core Hadoop dependencies, and arrange for task
dependencies to be available to remote tasks.

## End-user job configuration

Some job parameters – such as numbers of reduce tasks – are highly specific to
operational deployment.  Hadoop best practices dictate making these parameters
configurable by end-users at run-time.

Applications may manually provide explicit access to specific parameters, but
the Hadoop core `Tool` and `ToolRunner` classes allow end-users to set arbitrary
Hadoop configuration parameters via command-line arguments.  The functions in
the `parkour.tool` namespace allow normal Clojure functions to be used as
`Tool`s, and allow running `Tool`s or functions – and parsing command-line
arguments – via the `ToolRunner`.

```clj
(require '[parkour.tool :as tool])

(defn tool
  [conf & args]
  ...)

(defn -main
  [& args]
  (System/exit (tool/run tool args)))
```

## Hadoop dependencies

The correct way to depend on the core Hadoop artifacts under Leiningen is via
the `:provided` profile.  Dependencies in the `:provided` profile are made
available during development, compilation, testing, etc., but are not included
in any final uberjars, and are marked with the Maven scope `provided` in POMs.

Libraries should specify the most recent stable version of Hadoop, have separate
profiles for testing with Hadoop 1.x vs 2.x, and/or have profiles for testing
under a profile of common Hadoop versions (as per Parkour’s own test matrix).
Applications should ideally specify the exact version of Hadoop being targeted
for deployment, or follow the guidelines for libraries if not known.

Because `provided` dependencies are not included in resulting artifacts, you
must run your application such that a system-installed Hadoop’s artifacts are on
the application’s class path.  There are two basic approaches, both provided by
the standard `hadoop` command-line interface:

- `hadoop jar` – The `hadoop jar` sub-command will run a JAR main class or
  main-less JAR command-line–specified class in a JVM configured for the
  associated Hadoop installation.
- `hadoop classpath` – The `hadoop classpath` sub-command will print to stdout
  the complete class path for the associated Hadoop installation’s core
  artifacts and dependencies.  This class path may be used to manually launch a
  JVM with the necessary class path, plus any other desired options.

## Deployment artifacts

Hadoop applications must configure all jobs to directly reference all task
dependency JARs, not simply arrange for them to be on the initial JVM class
path.  The simplest way to accomplish this is by building whole-application
uberjars, which include all application code and dependencies in a single JAR
archive.  Parkour does nothing to interfere with other Hadoop-supported
deployment patterns, but Parkour’s default helpers and documentation assume
uberjar-based deployment.

The standard Leiningen `lein uberjar` sub-command will build an appropriate
uberjar, so long as the desired Hadoop dependencies are specified in the
`:provided` profile, as discussed above.
