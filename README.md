# Wolverine

An embedded, durable NoSQL database with support for schemas, full text search, and aggregation

    go get -u github.com/autom8ter/wolverine

## Noteable Libraries

- [badgerdb](github.com/dgraph-io/badger/v3) - key/value storage
- [bleve](github.com/blevesearch/bleve) - search indexing
- [gjson](github.com/tidwall/gjson) - json extraction utilities
- [sjson](github.com/tidwall/sjson) - json mutation utilities
- [lo](github.com/samber/lo) - generics
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

- [x] Core logic can be wrapped with middlewares for enhanced functionality
- [x] Embedded javascript middleware functions available for adding functionality without needing to recompile
- [x] Change streams available for integration with external systems
- [ ] Dedicated extensions library

### Roadmap

- [ ] codegen from collection schema
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
- [ ] SQL-like query language
- [ ] views
- [ ] materialized views
- [ ] multi-field order by
- [ ] distributed (raft)

## Getting Started

    go get -u github.com/autom8ter/wolverine

Create a [json schema](https://json-schema.org/):

```json
{
  "$id": "https://example.com/user.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "user",
  "type": "object",
  "@collection": "user",
  "@indexing": {
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
  },
  "@flags": {},
  "@annotations": {},
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

Instantiate a collection:


    userSchema := core.NewCollectionFromBytesP([]byte(schema))


Instantiate a database instance:

	config := wolverine.Config{
		StoragePath: "", // leave empty for in memory
		Collections: []*schema.Collection{userSchema},
		// add as many custom middlewares as needed
		Middlewares: []core.Middleware{{
			Persist:      []core.PersistWare{},
			Aggregate:    []core.AggregateWare{},
			Search:       []core.SearchWare{},
			Query:        []core.QueryWare{},
			Get:          []core.GetWare{},
			GetAll:       []core.GetAllWare{},
			ChangeStream: []core.ChangeStreamWare{},
		}},
	}
	db, err := wolverine.New(context.Background(), config)
	if err != nil {
		panic(err)
	}

## Document Collection Schema Properties

Each document collection is configured via a JSON Schema document with the following custom properties:

| property                  | description                                | required |
|---------------------------|--------------------------------------------|----------|
| @collection               | the name of the collection                 | true     |
| @indexing                 | custom query and search index entries      | false    |
| @primary                  | the document's primary key                 | true     |
| @indexing.query           | an array of query indexes  (order matters) | false    |
| @indexing.query[].fields  | an array of fields to index                | false    |
| @indexing.search          | an array of search indexes                 | false    |
| @indexing.search[].fields | an array of fields to index                | false    |
| @flags                    | arbitrary key(string)value(string) pairs   | false    |
| @annotations              | arbitrary key(string)value(string) pairs   | false    |

## Limitations

- Search enabled collections have poor write performance. Only add search indexes if you really need them.
- 

## Contributing

Install Dependencies

    go mod download

Run Tests

    go test -race -covermode=atomic -coverprofile=coverage.out ./...

Run Benchmarks

    go test -bench=. -benchmem -run=^#

Lint Repository

    golangci-lint run