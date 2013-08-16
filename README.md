# parkour

Hadoop MapReduce in idiomatic Clojure.

## Rationale

Cascading and Cascalog are excellent pieces of engineering, but
introduce significant complexity.  Each component layers on its own
abstractions, and the result is at significant distance from the
underlying Hadoop platform, while still depending on all its
specifics.  This makes it more difficult to reason about job execution
and more difficult to write performant jobs.

As a Clojure integration layer written directly against Hadoop,
parkour expresses the following properties:

- Functional.  Many abstractions in Hadoop and Cascading are
  ultimately just functions in object clothing.  In parkour, Mappers,
  Reducers, Partitioners, and Cascading’s many flavors of Operation
  are all replaced with plain Clojure functions.
- Dynamic.  Hadoop and Cascading reconstitute remote task code by
  performing reflection or Java deserialization on instances of
  concrete types.  This approach is antithetical to idiomatic Clojure
  code, which tends to involve run-time generation of non-seralizable
  closures.  In parkour, task steps are reconstituted by requiring
  namespaces and invoking vars, which eliminates the need for concrete
  types or even ahead-of-time compilation.
- Idiomatic.  Any Clojure code base will already have many calls to
  the functions named `map` and `reduce`.  In parkour, these functions
  sit at the heart of MapReduce tasks, acting on distributed remote
  sequences exactly as they would on local ones.

## Usage

FIXME

## Uncookedbook

These are things parkour should be able to do:

  - Multiple reducer outputs.
  - Multiple mappers consuming multiple inputs.
  - Group by E2LD for reduction, using a side-data ETLD list.
  - Re-usable distributed counting and generation of unique indices as
    for a matrix.  Support arbitrary types being counted and indexed.

## License

Copyright © 2013 Damballa, Inc. & Marshall Bockrath-Vandegrift

Distributed under the Eclipse Public License, the same as Clojure.
