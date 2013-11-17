# Parkour News – history of user-visible changes

## 0.5.0 / ???

### Breaking changes

- The default map/reduce task function interface to uses the new `collfn`
  adapter.  The previous interface may be specified via the `contextfn` adapter.
- Local reduction of dseqs yields unwrapped values.  Raw values may be accessed
  via `source-for` with the `:raw?` option.

### Other changes

- Allow seqs to be used as tuple sources.
- Allow chaining of `sink-as` results to source-shaping functions.
- Allow var entry points to directly specify the adapter function used to
  transform their values to the type-specific Parkour base interface.

## 0.4.1 / 2013-11-15

- Fix bug in `reduced` handling of job tuple-source reductions.
- Make tuple-source re-shape results `seq`able, allowing tasks to be written in
  terms of lazy sequences.

## 0.4.0 / 2013-11-04

- Initial public release.
