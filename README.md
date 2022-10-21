# Wolverine

An embedded NoSQL database with support for full text search and indexing.

Design extremely simple stateful microservices with zero external dependencies

Feels like a mongodb+elasticsearch+redis stack but embedded, allowing you to create data-intensive Go programs with zero
external dependencies for data persistance(just disc or memory)

Built on top of BadgerDB and Bleve

    go get -u github.com/autom8ter/wolverine

Features:

## Search Engine

- [x] prefix
- [x] basic
- [x] regex
- [x] wildcard
- [x] term range
- [x] date range
- [x] geo distance
- [x] boosting
- [x] select fields

## Document Storage Engine

- [x] document storage engine
- [x] json schema based validation & configuration
- [x] field based querying
- [x] change streams
- [x] batch operations (set/get/update)
- [x] write hooks
- [x] multi-field indexing
- [x] select fields
- [x] order by
- [x] aggregation (min,max,sum,avg,count)
    - [x] min
    - [x] max
    - [x] count
    - [x] avg
    - [x] sum
    - [x] group by
- [x] query update
- [x] query delete
- [ ] multi-field order by
- [x] pagination

## System/Admin Engine

- [x] backup
- [x] incremental backup
- [x] restore
- [x] migrations
- [ ] distributed (raft)

## Road to Beta

- [ ] awesome readme
- [ ] benchmarks
- [ ] examples
- [ ] better errors & error codes
- [ ] 80% test coverage
- [ ] extensive comments
- [ ] aggregate indexes

## Beta+ Roadmap

- [ ] SQL-like query language
- [ ] views
- [ ] materialized views