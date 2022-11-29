# gokvkit

A framework for building powerful, extensible, and feature-rich microservices on top of key/value storeage

    go get -u github.com/autom8ter/gokvkit


## Use Case

Build stateful, extensible, and feature-rich programs on top of pluggable key/value storage providers

## Features:

- [x] JSON document storage engine
  - built on top of pluggable key/value storage
- [x] field based querying
- [x] batch operations (create/set/get/update)
- [x] query batch operations (create/set/get/update)
- [x] multi-field indexing
- [x] pluggable optimizer
- [x] unique indexes/constraints
- [x] select fields
- [x] multi-field order by
- [x] aggregation (min,max,sum,count)
    - [x] min
    - [x] max
    - [x] count
    - [x] sum
    - [x] group by
- [x] pagination
- [x] hook-based functions
  - [x] validation hooks (on write)
    - [x] json schema based validation
  - [x] read hooks (on read)
  - [x] sideEffect Hooks (on write)

### Flexibility
- [x] Run in memory for ease of testing
- [x] Run in local storage when persistance is needed (badger provider)
- [ ] Run against a distributed key value store when scalability/stateless AND persistance is needed (tikv provider)
- [x] Query optimizer is pluggable

### Extensibility

- [x] Core logic can be extended with functional hooks
  - [x] validation hooks (on write)
    - [x] json schema based validation
  - [x] read hooks (on each document read)
  - [x] sideEffect hooks (on write)
  - [x] where hooks (on query)
- [ ] Dedicated extensions library

### Performance
- [x] Secondary Indexes drastically improve performance and reduce likelihood of full table scans

## Getting Started

    go get -u github.com/autom8ter/gokvkit

#### Configuring a database instance

WIP

#### Adding documents to a collection

WIP

#### Reading documents from a collection

WIP

#### Querying documents from a collection

WIP

#### Aggregating documents from a collection

WIP

#### Adding hooks to a collection

WIP


### Beta Roadmap

- [ ] transactions
- [ ] 80% test coverage
- [ ] better errors & error codes
- [ ] 80% test coverage
- [ ] examples
- [ ] awesome readme
- [ ] cicd
- [ ] migrations
- [ ] benchmarks

### Beta+ Roadmap

- [ ] multi-field primary key
- [ ] search indexing
- [ ] external data importer


## Contributing

Install Dependencies

    go mod download

Checkout Branch

    git checkout -b ${issueNumber}

Run Tests

    go test -race -covermode=atomic -coverprofile=coverage.out ./...

Run Benchmarks

    go test -bench=. -benchmem -run=^#

Lint Repository

    golangci-lint run

Check Coverage

    go tool cover -func=coverage.out