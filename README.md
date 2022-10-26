# Wolverine

An embedded NoSQL database with support for JSON schemas, full text search, and aggregation

    go get -u github.com/autom8ter/wolverine

## Noteable Libraries

- [badgerdb](github.com/dgraph-io/badger/v3) - key/value storage
- [bleve](github.com/blevesearch/bleve) - search indexing
- [gjson](github.com/tidwall/gjson) - json extraction utilities
- [sjson](github.com/tidwall/sjson) - json mutation utilities
- [lo](github.com/samber/lo) - generic collection functions
- [machine](github.com/autom8ter/machine/v4) - in-memory publish/subscribe functionality
- [jsonschema](github.com/qri-io/jsonschema) - json schema support
- [goja](github.com/dop251/goja) - embedded javascript runtime

## Use Case

Build powerful, extensible, and feature-rich microservices without database dependencies

## Features:

### Search Engine

- [x] prefix
- [x] basic
- [x] regex
- [x] wildcard
- [x] term range
- [x] field boosting

### Document Storage Engine

- [x] JSON document storage engine
- [x] custom json schema based validation & configuration
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
- [x] pagination

### System/Admin Engine

- [x] backup
- [x] restore
- [x] reindex

### Extensibility

- [x] core logic can be wrapped with middlewares for enhanced functionality
- [x] embedded javascript middleware functions available for adding functionality without needing to recompile
- [x] change streams available for integration with external systems
- [ ] dedicated extensions library

### Road to Beta

- [ ] unique constraints
- [ ] external data importer
- [ ] incremental backup
- [ ] migrations
- [ ] better errors & error codes
- [ ] cicd
- [ ] awesome readme
- [ ] benchmarks
- [ ] examples
- [ ] 80% test coverage
- [ ] extensive comments

### Beta+ Roadmap

- [ ] SQL-like query language
- [ ] views
- [ ] materialized views
- [ ] multi-field order by
- [ ] distributed (raft)

# Getting Started

Create a collection schema:

```json
{
  "$id": "https://example.com/user.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "User",
  "type": "object",
  "@config": {
    "collection": "user",
    "indexing": {
      "query": [
        {
          "fields": [
            "contact.email"
          ]
        },
        {
          "fields": [
            "account_id"
          ]
        },
        {
          "fields": [
            "language"
          ]
        }
      ],
      "search": [
        {
          "fields": [
            "contact.email"
          ]
        }
      ]
    }
  },
  "required": [
    "_id",
    "name",
    "age",
    "contact",
    "gender",
    "account_id"
  ],
  "properties": {
    "_id": {
      "@primary": true,
      "type": "string",
      "description": "The user's id."
    },
    "name": {
      "type": "string",
      "description": "The user's name."
    },
    "contact": {
      "type": "object",
      "properties": {
        "email": {
          "type": "string",
          "description": "The user's email."
        }
      }
    },
    "age": {
      "description": "Age in years which must be equal to or greater than zero.",
      "type": "integer",
      "minimum": 0
    },
    "account_id": {
      "type": "integer",
      "minimum": 0
    },
    "language": {
      "type": "string",
      "description": "The user's first language."
    },
    "gender": {
      "type": "string",
      "description": "The user's gender.",
      "enum": [
        "male",
        "female"
      ]
    },
    "timestamp": {
      "type": "string"
    },
    "annotations": {
      "type": "object"
    }
  }
}
```

Instantiate a database instance:

```go

```

# Contributing

Install Dependencies

    go mod download

Run Tests

    go test -race -covermode=atomic -coverprofile=coverage.out ./...

Run Benchmarks

    go test -bench=. -benchmem -run=^#

Lint Repository

    golangci-lint run