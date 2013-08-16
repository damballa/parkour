# Goals

The goals of the Parkour project are to:

  1. Support writing Hadoop MapReduce jobs in idiomatic Clojure.
  2. Allow everything it is possible to do on Hadoop in Java to be
     done in Clojure.
  3. Provide interfaces and abstractions which build incrementally
     atop the interfaces and abstractions provided by Hadoop itself.
  4. Support composing multiple Hadoop jobs without needing to
     manually link each intermediate job’s input & output location.
  5. Transparently serialize Clojure data in a fashion which supports
     efficient sorting.
  6. Support building job-chains using the Clojure `reducers` library
     over a “collection” type representing MapReduce input sources.
