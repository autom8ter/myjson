# gokvkit [![GoDoc](https://godoc.org/github.com/autom8ter/gokvkit?status.svg)](https://godoc.org/github.com/autom8ter/gokvkit)
![Coverage](https://img.shields.io/badge/Coverage-72.7%25-brightgreen)

gokvkit is an embedded database built on top of pluggable key value storage

    go get -u github.com/autom8ter/gokvkit

## Use Case

Build stateful, extensible, and feature-rich programs on top of pluggable key/value storage providers

## Features:

### Architecture

| Feature                            | Description                                                                                    | Implemented |
|------------------------------------|------------------------------------------------------------------------------------------------|-------------|
| Single Node (in-memory)            | Run the embedded database with no persistance on a single node                                 | [x]         |
| Single Node (on-disk)              | Run the embedded database with persistance on a single node                                    | [x]         |
| Distributed (distributed kv-store) | Run the embedded database with horizontal scalability using tikv distributed key value storage | [x]         |

### Database

| Feature           | Description                                                                                                           | Implemented |
|-------------------|-----------------------------------------------------------------------------------------------------------------------|-------------|
| JSON Documents    | Records are stored as JSON documents                                                                                  | [x]         |
| Collections       | Records are stored in Collections which can hold any number of JSON documents                                         | [x]         |
| Collection Schema | Collections define a JSON Schema which enforces the schema of JSON documents in a collection                          | [x]         |
| Transactions      | Cross Collection transactions can be used to persist/rollback changes to the database                                 | [x]         |
| Change Streams    | Built in Change-Data-Capture collection can be queried & streamed for triggering realtime events                      | [x]         |
| Scripting         | Javascript scripts can be executed with full access to database functionality                                         | [x]         |
| Triggers          | Javascript triggers can be configured at the collection level to add custom business logic based on when events occur | [x]         |
| Migrations        | Built in support for atomic database migrations written in javascript                                                 | [x]         |
| Relationships     | Built in support for relationships with foreign keys - Joins and cascade deletes are also supported                   | [x]         |
| Secondary Indexes | Multi-field secondary indexes may be used to boost query performance (eq/gt/lt/gte/lte)                               | [x]         |
| Unique Fields     | Unique fields can be configured which ensure the uniqueness of a field value in a collection                          | [x]         |
| Complex Queries   | Complex queries can be executed with support for select/where/join/having/orderby/groupby/limit/page clauses          | [x]         |
| Aggregate Queries | Complex aggregate queries can be executed for analytical purposes                                                     | [x]         |

### Storage Providers

| Provider | Description                                           | Implemented |
|----------|-------------------------------------------------------|-------------|
| Badger   | persistant, embedded LSM database written in Go       | [x]         |
| Tikv     | persistant, distributed LSM database  written in Rust | [x]         |
| RocksDB  | persistant, embedded LSM database written in C++      |             |



## Getting Started

    go get -u github.com/autom8ter/gokvkit

Before getting started, take a look at the [examples](./examples)

#### Configuring a database instance

Collection schemas can be configured at runtime or at startup. Collection schemas are declarative - 
any changes to indexing or validation happen within the database when ConfigureCollection is called

```go

var (
    //go:embed account.yaml
    accountSchema string
    //go:embed user.yaml
    userSchema string
    //go:embed task.yaml
    taskSchema string
)

if err := db.ConfigureCollection(ctx, []byte(accountSchema)); err != nil {
	panic(err)
}
if err := db.ConfigureCollection(ctx, []byte(userSchema)); err != nil {
	panic(err)
}
if err := db.ConfigureCollection(ctx, []byte(taskSchema)); err != nil {
	panic(err)
}
```

### Creating a JSON document

```go
document, err := gokvkit.NewDocumentFrom(map[string]any{
    "name": "acme.com",
})
```

```go
doc := gokvkit.NewDocument()
doc.Set("name", "acme.com")
```


### Transactions


Most database functionality is made available via the Tx interface which has read/write methods
across 1-many collections.

#### Writable
```go
if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx gokvkit.Tx) error {
	// do stuff ...tx.Set(...)
	// return error to rollback
	// return no error to commit
}
```

#### Read Only

```go
if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: true}, func(ctx context.Context, tx gokvkit.Tx) error {
	// ...tx.Get(...)
}
```

#### Adding documents to a collection

```go
if err := db.Tx(ctx, kv.TxOpts{}, func(ctx context.Context, tx gokvkit.Tx) error {
    doc := gokvkit.NewDocument()
    doc.Set("name", "acme.com")
	id, err := tx.Create(ctx, "account", document)
	if err != nil {
		return err
    }
}
```

### Queries

```go
results, err := tx.Query(ctx, "user", gokvkit.Q().
    Select(gokvkit.Select{Field: "*"}).
	OrderBy(gokvkit.OrderBy{Field: "age", Direction: gokvkit.OrderByDirectionDesc}).
    Query())
```

#### Joins

```go
results, err := db.Query(ctx, "user", gokvkit.Q().
    Select(
        gokvkit.Select{Field: "acc._id", As: "account_id"},
        gokvkit.Select{Field: "acc.name", As: "account_name"},
		gokvkit.Select{Field: "_id", As: "user_id"},
    ).
    Join(gokvkit.Join{
        Collection: "account",
        On: []gokvkit.Where{
            {
				Field: "_id",
				Op:    gokvkit.WhereOpEq,
                Value: "$account_id",
            },
    },
        As: "acc",
    }).
Query())
```


#### Iterating through documents in a collection

```go
_, err := tx.ForEach(ctx, "user", gokvkit.ForEachOpts{}, func(d *gokvkit.Document) (bool, error) {
    fmt.Println(d)
    return true, nil
})
```


#### Reading documents in a collection

```go
doc, err := tx.Get(ctx, "user", "$id")
```

### Change Streams

#### Stream Changes in a given collection

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
err := db.ChangeStream(ctx, "user", func(cdc gokvkit.CDC) (bool, error) {
    fmt.Println(cdc)
    return true, nil
})
```

### Aggregation

```go
query := gokvkit.Query{
	Select: []gokvkit.Select{
		{
            Field: "account_id",
		},
		{
            Field:     "age",
            Aggregate: gokvkit.AggregateFunctionSum,
            As:        "age_sum",
		},
	},
	GroupBy: []string{"account_id"},
}
results, err := db.Query(ctx, "user", query)
```

### Triggers

add a triggers block to your JSON schema
ex: update timestamp on set/update/create
```yaml
triggers:
  set_timestamp:
    order: 1
    events:
      - on_create
      - on_update
      - on_set
    script: |
      doc.set('timestamp', new Date().toISOString())
```

## Benchmarks

WIP

## Tikv Setup Guide (full scale)

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

Check Coverage

    go tool cover -func=coverage.out