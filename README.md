# Brutus

A framework for building powerful, extensible, and feature-rich microservices on top of key/value storeage

    go get -u github.com/autom8ter/brutus


## Use Case

Build powerful, extensible, and feature-rich microservices on top of pluggable key/value storage providers

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
- [x] order by
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


### Extensibility

- [x] Core logic can be extended with functional hooks
- [ ] Change streams available for integration with external systems
- [ ] Dedicated extensions library

### Roadmap

- [ ] change streams
- [ ] multi-field order by
- [ ] multi-field primary key
- [ ] search indexes
- [ ] external data importer
- [ ] migrations
- [ ] better errors & error codes
- [ ] cicd
- [ ] awesome readme
- [ ] benchmarks
- [ ] examples
- [ ] 80% test coverage
- [ ] extensive comments
- [ ] SQL-like query language

## Getting Started

    go get -u github.com/autom8ter/brutus

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

#### Streaming documents from a collection

WIP

#### Streaeming documents from a collection

WIP



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
