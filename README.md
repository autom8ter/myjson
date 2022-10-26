# Wolverine

An embedded NoSQL database with support for full text search and indexing.

Design extremely simple stateful microservices with zero external dependencies

Feels like a mongodb+elasticsearch+redis stack but embedded, allowing you to create data-intensive Go programs with zero
external dependencies for data persistance(just disc or memory)

Built on top of BadgerDB and Bleve

    go get -u github.com/autom8ter/wolverine

## Noteable Libraries

- [badgerdb](github.com/dgraph-io/badger/v3) - key/value storage
- [bleve](github.com/blevesearch/bleve) - search indexing
- [gjson](github.com/tidwall/gjson) - json extraction utilities
- [sjson](github.com/tidwall/sjson) - json mutation utilities
- [lo](github.com/samber/lo) - generic collection functions
- [machine](github.com/autom8ter/machine/v4) - in-memory publish/subscribe functionality
- [jsonschema]() - json schema support

## Use Case

TODO

## Features:

### Search Engine

- [x] prefix
- [x] basic
- [x] regex
- [x] wildcard
- [x] term range
- [x] boosting
- [x] select fields

### Document Storage Engine

- [x] document storage engine
- [x] json schema based validation & configuration
- [x] field based querying
- [x] change streams
- [x] batch operations (create/set/get/update)
- [x] multi-field indexing
- [x] select fields
- [x] order by
- [x] aggregation (min,max,sum,avg,count)
    - [x] min
    - [x] max
    - [x] count
    - [ ] avg
    - [x] sum
    - [x] group by
- [x] query update
- [x] query delete
- [ ] multi-field order by
- [x] pagination

### System/Admin Engine

- [x] backup
- [x] restore
- [x] reindex
- [ ] incremental backup
- [ ] migrations
- [ ] distributed (raft)

### Extensibility

- [x] core logic can be wrapped with middlewares for enhanced functionality
- [x] embedded javascript middleware functions available for adding functionality without needing to recompile
- [x] change streams available for integration with external systems

### Road to Beta

- [ ] awesome readme
- [ ] benchmarks
- [ ] examples
- [ ] better errors & error codes
- [ ] 80% test coverage
- [ ] extensive comments
- [ ] cicd

### Beta+ Roadmap

- [ ] SQL-like query language
- [ ] views
- [ ] materialized views

## Contributing

TODO