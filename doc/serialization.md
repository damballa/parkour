# Serialization in Parkour

Parkour works directly with whatever serialization mechanisms are already
registered with Hadoop.  Very little of how Parkour handles serialization is
specific to Parkour.

## The Wrapper protocol

Hadoop’s `Writable` types establish the most common idiom for handling
serialization on Hadoop.  In this idiom, tasks hold single mutable instances of
serialization-specific wrapper types.  These instances are mutated for each
input or output key/value.  

Parkour makes this pattern concrete in the `parkour.wrapper` namespaces’s
`Wrapper` protocol.  The `Wrapper` protocol provides two functions:

- `unwrap` – Accepts a wrapper type instance; returns the wrapped value.
  Defaults to returning the “wrapper” object itself.
- `rewrap` – Accepts a wrapper type instance and a new value; returns a wrapper
  type instance holding the wrapped value.  Defaults to returning the value
  object itself.
  
In task functions, by default, input tuples are `unwrap`ed to values and output
tuples are `rewrap`ed to the configured task output types.  This behavior is
optional, and may be disabled via `:parkour.mapreduce/raw` metadata on
task-vars.  The protocol functions may also be called manually, and many
Parkour/Hadoop types are extended to do the obvious thing when `unwrap`ed.

## Specific serializations

All serializations in Hadoop are considered equal, but some are more equal than
others.

### Avro

Although nothing embedded in Parkour’s design or implementation gives it special
status, Avro is the preferred Parkour Hadoop serialization method.

Unlike most common Hadoop serialization mechanisms, Avro does not require
ahead-of-time compilation, code-generation, or concrete types.  With
[Abracad][abracad] and Avro >=1.7.5, Parkour can configure Hadoop Avro
serialization to de/serialize directly to and from Clojure data structures.
Avro is schema-based, but Abracad allows schemas to be specified in-line as
Clojure data structures, and supports automatic generation of schemas which can
handle arbitrary EDN-serializable Clojure data.

Unlike other serialization formats which offer similar Clojure-wise usability
benefits, Avro was originally designed for Hadoop, and will automatically
provide an efficient byte-wise comparison operation (Hadoop `RawComparator`) for
any configured schema.  Parkour further allows job specification of a grouping
schema which differs in sort parameters from the base key schema.

[abracad]: https://github.com/damballa/abracad/
