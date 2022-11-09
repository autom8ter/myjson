# Brutus

A framework for building powerful, extensible, and feature-rich microservices on top of key/value storeage

    go get -u github.com/autom8ter/brutus

## Noteable Libraries

- [gjson](github.com/tidwall/gjson) - json extraction utilities
- [sjson](github.com/tidwall/sjson) - json mutation utilities
- [lo](github.com/samber/lo) - generics
- [machine](github.com/autom8ter/machine/v4) - in-memory publish/subscribe functionality
- [jsonschema](github.com/qri-io/jsonschema) - json schema support

## Use Case

Build powerful, extensible, and feature-rich microservices on top of key/value storeage

## Features:

- [x] JSON document storage engine
- [x] validation hooks
  - [x] json schema based validation
- [x] field based querying
- [x] batch operations (create/set/get/update)
- [x] query batch operations (create/set/get/update)
- [x] multi-field indexing
- [x] unique indexes/constraints
- [x] select fields
- [x] order by
- [x] aggregation (min,max,sum,avg,count)
    - [x] min
    - [x] max
    - [x] count
    - [x] sum
    - [x] group by
- [x] pagination

### Extensibility

- [x] Core logic can be wrapped with middlewares for enhanced functionality
- [ ] Change streams available for integration with external systems
- [ ] Dedicated extensions library

### Roadmap
- [ ] change streams
- [ ] multi-field order by
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
