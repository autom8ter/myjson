# Wolverine

An embedded NoSQL database with support for full text search and indexing.

Design extremely simple stateful microservices with zero external dependencies

Feels like a mongodb+elasticsearch+redis stack but embedded, allowing you to create data-intensive Go programs with zero
external dependencies for data persistance(just disc or memory)

Built on top of BadgerDB and Bleve

    go get github.com/autom8ter/wolverine

Features:

- [x] full text search engine
    - [x] prefix
    - [x] basic
    - [x] regex
    - [x] term
    - [x] boolean
    - [x] term range
    - [x] date range
    - [x] numeric range
- [x] document storage engine
- [x] kv cache engine supporting ttl/expiration
- [x] change streams
- [x] field based querying
- [x] batch operations
- [x] ttl
- [x] hooks
    - [x] on read
    - [x] on stream
    - [x] before update
    - [x] after update
    - [x] before set
    - [x] after set
    - [x] before delete
    - [x] after delete
- [x] field based indexes
- [x] ttl
- [x] hooks
- [x] select fields
- [x] order by
- [x] mapreduce
- [x] cron jobs
- [x] backup
- [x] restore
- [x] query update
- [x] query delete
- [x] logger
- [x] migrations
- [x] optional json-schema validation

## Road to Beta

- [ ] decouple kv storage provider
- [ ] benchmarks
- [ ] examples
- [ ] http server(gokit)
- [ ] 90% test coverage

## Beta+ Roadmap

- [ ] multi-field order by
- [ ] distributed
- [ ] SQL support