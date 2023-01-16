# myjson [![GoDoc](https://godoc.org/github.com/autom8ter/myjson?status.svg)](https://godoc.org/github.com/autom8ter/myjson)
![Coverage](https://img.shields.io/badge/Coverage-72.5%25-brightgreen)

```
███    ███ ██    ██      ██ ███████  ██████  ███    ██ 
████  ████  ██  ██       ██ ██      ██    ██ ████   ██ 
██ ████ ██   ████        ██ ███████ ██    ██ ██ ██  ██ 
██  ██  ██    ██    ██   ██      ██ ██    ██ ██  ██ ██ 
██      ██    ██     █████  ███████  ██████  ██   ████ 
                                                       
```

MyJSON is an embedded relational document store built on top of pluggable key value storage

    go get -u github.com/autom8ter/myjson

- [Use Case](#use-case)
- [Features:](#features-)
  * [Architecture](#architecture)
  * [Database](#database)
  * [Storage Providers](#storage-providers)
- [Getting Started](#getting-started)
  * [Collection Schemas](#collection-schemas)
    + [Custom Root Level Properties](#custom-root-level-properties)
    + [Custom Field Level Properties](#custom-field-level-properties)
    + [Example Schema](#example-schema)
  * [Opening a database instance](#opening-a-database-instance)
    + [Single Node in Memory (badger)](#single-node-in-memory--badger-)
    + [Single Node w/ Persistance (badger)](#single-node-w--persistance--badger-)
    + [Multi Node w/ Persistance (tikv)](#multi-node-w--persistance--tikv-)
  * [Configuring a database instance](#configuring-a-database-instance)
  * [Working with JSON documents](#working-with-json-documents)
    + [Creating a JSON document](#creating-a-json-document)
    + [Setting JSON values](#setting-json-values)
    + [Getting JSON values](#getting-json-values)
  * [Transactions](#transactions)
    + [Writable](#writable)
    + [Read Only](#read-only)
    + [Adding documents to a collection](#adding-documents-to-a-collection)
  * [Queries](#queries)
    + [Joins](#joins)
    + [Iterating through documents in a collection](#iterating-through-documents-in-a-collection)
    + [Reading documents in a collection](#reading-documents-in-a-collection)
  * [Change Streams](#change-streams)
    + [Stream Changes in a given collection](#stream-changes-in-a-given-collection)
  * [Aggregation](#aggregation)
  * [Triggers](#triggers)
  * [Scripts](#scripts)
- [Tikv Setup Guide (full scale)](#tikv-setup-guide--full-scale-)
- [Contributing](#contributing)

## Use Case

Build powerful applications on top of simple key value storage. 

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
| Time Travel       | Find the value of a   document at a specific timestamp                                                                | [x]         |
| Revert            | Revert the value of a document to it's value at a specific timestamp                                                  | [x]         |

### Storage Providers

| Provider | Description                                           | Implemented |
|----------|-------------------------------------------------------|-------------|
| Badger   | persistant, embedded LSM database written in Go       | [x]         |
| Tikv     | persistant, distributed LSM database  written in Rust | [x]         |
| RocksDB  | persistant, embedded LSM database written in C++      |             |



## Getting Started

    go get -u github.com/autom8ter/myjson

Before getting started, take a look at the [examples](./examples) and [Godoc](https://godoc.org/github.com/autom8ter/myjson)

### Collection Schemas

Collection schemas are custom [JSON Schema](https://json-schema.org/) documents that declaratively configure a collection of documents. The
schema defines the structure of the JSON documents in the collection and the constraints that
should be enforced on the documents. The schema is used to validate the documents in the collection
and to enforce the constraints on the documents. The schema is also used to define secondary indexes,
foreign keys, authorization, and triggers.

#### Custom Root Level Properties

| Property        | Description                                                                                                      | Required |
|-----------------|------------------------------------------------------------------------------------------------------------------|----------|
| x-collection    | x-collection configures the unique name of the collection                                                        | [x]      |
| x-authorization | x-authorization is a set of javascript authorization rules to enforce against actions taken against the database | [ ]      |
| x-triggers      | x-triggers are a set of javascript triggers that execute when certain actions are taken against the database     | [ ]      |
| x-read-only     | x-readonly indicates that the collection is updated internally and may only be read                              | [ ]      |

#### Custom Field Level Properties


| Property  | Description                                                                             |
|-----------|-----------------------------------------------------------------------------------------|
| x-primary | x-primary configures the documents primary key (exactly 1 field must specify x-primary) |
| x-foreign | x-foreign configures a foreign key relationship                                         |
| x-unique  | x-unique configures a unique field for the document                                     |
| x-index   | x-index configures a secondary index                                                    |


#### Example Schema

account.yaml
```yaml
type: object
x-collection: account
required:
  - _id
  - name
properties:
  _id:
    type: string
    description: The account's id.
    x-primary: true
  name:
    type: string
    description: The accounts's name.
x-authorization:
  rules:
    ## allow super users to do anything
    - effect: allow
      ## match on any action
      action:
      - "*"
      ## context metadata must have role super_user set to true
      match: |
        contains(meta.Get('roles'), 'super_user')

      ## dont allow read-only users to create/update/delete/set accounts
    - effect: deny
      ## match on document mutations
      action:
        - create
        - update
        - delete
        - set
        ## context metadata must have a read_only role set to true
      match: |
        contains(meta.Get('roles'), 'read_only')

      ## only allow users to update their own account
    - effect: allow
        ## match on document mutations
      action:
        - create
        - update
        - delete
        - set
        ## the account's _id must match the user's account_id
      match: |
        doc.Get('_id') == meta.Get('account_id')

      ## only allow users to query their own account
    - effect: allow
        ## match on document queries (includes ForEach and other Query based methods)
      action:
        - query
        ## user must have a group matching the account's _id
      match: |
        query.where?.length > 0 && query.where[0].field == '_id' && query.where[0].op == 'eq' && contains(meta.Get('groups'), query.where[0].value)

```

user.yaml

```yaml
type: object
# x-collection specifies the name of the collection the object will be stored in
x-collection: user
# required specifies the required attributes
required:
  - _id
  - name
  - age
  - contact
  - gender
  - account_id
properties:
  _id:
    type: string
    description: The user's id.
    # x-primary indicates that the property is the primary key for the object - only one primary key may be specified
    x-primary: true
  name:
    type: string
    description: The user's name.
  contact:
    type: object
    properties:
      email:
        type: string
        description: The user's email.
        x-unique: true
  age:
    description: Age in years which must be equal to or greater than zero.
    type: integer
    minimum: 0
  account_id:
    type: string
    # x-foreign indicates that the property is a foreign key - foreign keys are automatically indexed
    x-foreign:
      collection: account
      field: _id
      cascade: true
    # x-index specifies a secondary index which can have 1-many fields
    x-index:
      account_email_idx:
        additional_fields:
          - contact.email
  language:
    type: string
    description: The user's first language.
    x-index:
      language_idx: { }
  gender:
    type: string
    description: The user's gender.
    enum:
      - male
      - female
  timestamp:
    type: string
  annotations:
    type: object

# x-triggers are javascript functions that execute based on certain events
x-triggers:
  # name of the trigger
  set_timestamp:
    # order determines the order in which the functions are executed - lower ordered triggers are executed first
    order: 1
    # events configures the trigger to execute on certain events
    events:
      - on_create
      - on_update
      - on_set
    # script is the javascript to execute
    script: |
      doc.Set('timestamp', new Date().toISOString())

```
task.yaml

```yaml
type: object
x-collection: task
required:
  - _id
  - user
  - content
properties:
  _id:
    type: string
    description: The user's id.
    x-primary: true
  user:
    type: string
    description: The id of the user who owns the task
    x-foreign:
      collection: user
      field: _id
      cascade: true
  content:
    type: string
    description: The content of the task
```


#### Authorization 

TODO

### Opening a database instance


#### Single Node in Memory (badger)
```go
db, err := myjson.Open(context.Background(), "badger", map[string]any{
	"storage_path": "",
})
```

#### Single Node w/ Persistance (badger)
```go
db, err := myjson.Open(context.Background(), "badger", map[string]any{
	"storage_path": "./tmp",
})
```


#### Multi Node w/ Persistance (tikv)
```go
db, err := myjson.Open(context.Background(), "tikv", map[string]any{
    "pd_addr":    []string{"http://pd0:2379"},
    "redis_addr": "localhost:6379",
    "redis_user": "admin", //change me
    "redis_password": "123232", //change me
})
```

### Configuring a database instance

Collection schemas can be configured at runtime or at startup. Collection schemas are declarative - 
any changes to indexing or validation happen within the database when Configure is called

```go

var (
    //go:embed account.yaml
    accountSchema string
    //go:embed user.yaml
    userSchema string
    //go:embed task.yaml
    taskSchema string
)

if err := db.Configure(ctx, map[string]any{
	"account": accountSchema,
    "user": userSchema,
    "task": taskSchema,
}); err != nil {
    panic(err)
})
```

### Working with JSON documents

#### Creating a JSON document

```go
document, err := myjson.NewDocumentFrom(map[string]any{
    "name": "acme.com",
})
```

```go
doc := myjson.NewDocument()
doc.Set("name", "acme.com")
```

#### Setting JSON values

```go
doc := myjson.NewDocument()
doc.Set("name", "acme.com")
```

SJSON syntax is supported: https://github.com/tidwall/sjson#path-syntax
```go
doc := myjson.NewDocument()
doc.Set("contact.email", "info@acme.com")
```


#### Getting JSON values

```go
doc := myjson.NewDocument()
doc.Set("name", "acme.com")
```

GJSON syntax is supported: https://github.com/tidwall/sjson#path-syntax
```go
value := doc.Get("contact.email")
```

additional GJSON modifiers are available:
- @camelCase - convert a json string field to camel case `doc.Get("project|@camelCase")`
- @snakeCase - convert a json string field to snake case `doc.Get("project|@snakeCase")`
- @kebabCase - convert a json string field to kebab case `doc.Get("project|@kebabCase")`
- @replaceAll - replace a substring within a json string field with another string 
- @unix - get the unix timestamp of the json time field `doc.Get("timestamp|@unix")`
- @unixMilli - get the unix millisecond timestamp of the json time field `doc.Get("timestamp|@unixMilli")`
- @unixNano - get the unix nanosecond timestamp of the json time field `doc.Get("timestamp|@unixNano")`
- @dateTrunc - truncate a date to day, month, or year ex: `doc.GetString("timestamp|@dateTrunc:month")`,  `doc.GetString("timestamp|@dateTrunc:year")`,  `doc.GetString("timestamp|@dateTrunc:day")`

### Transactions

Most database functionality is made available via the Tx interface which has read/write methods
across 1-many collections.

#### Writable
```go
if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: false}, func(ctx context.Context, tx myjson.Tx) error {
	// do stuff ...tx.Set(...)
	// return error to rollback
	// return no error to commit
}
```

#### Read Only

```go
if err := db.Tx(ctx, kv.TxOpts{IsReadOnly: true}, func(ctx context.Context, tx myjson.Tx) error {
	// ...tx.Get(...)
}
```

#### Adding documents to a collection

```go
if err := db.Tx(ctx, kv.TxOpts{}, func(ctx context.Context, tx myjson.Tx) error {
    doc := myjson.NewDocument()
    doc.Set("name", "acme.com")
	id, err := tx.Create(ctx, "account", document)
	if err != nil {
		return err
    }
}
```

### Queries

```go
results, err := tx.Query(ctx, "user", myjson.Q().
    Select(myjson.Select{Field: "*"}).
	OrderBy(myjson.OrderBy{Field: "age", Direction: myjson.OrderByDirectionDesc}).
    Query())
```

#### Joins

1-many joins are 
```go
results, err := db.Query(ctx, "user", myjson.Q().
    Select(
        myjson.Select{Field: "acc._id", As: "account_id"},
        myjson.Select{Field: "acc.name", As: "account_name"},
		myjson.Select{Field: "_id", As: "user_id"},
    ).
    Join(myjson.Join{
        Collection: "account",
        On: []myjson.Where{
            {
				Field: "_id",
				Op:    myjson.WhereOpEq,
                Value: "$account_id", //self reference the account_id on the user document
            },
    },
        As: "acc",
    }).
Query())
```


#### Iterating through documents in a collection

```go
_, err := tx.ForEach(ctx, "user", myjson.ForEachOpts{}, func(d *myjson.Document) (bool, error) {
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

CDC persistance must be enabled for change streams to work. See the database Options for more info.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
err := db.ChangeStream(ctx, "user", func(cdc myjson.CDC) (bool, error) {
    fmt.Println(cdc)
    return true, nil
})
```

### Aggregation

```go
query := myjson.Query{
	Select: []myjson.Select{
		{
            Field: "account_id",
		},
		{
            Field:     "age",
            Aggregate: myjson.AggregateFunctionSum,
            As:        "age_sum",
		},
	},
	GroupBy: []string{"account_id"},
}
results, err := db.Query(ctx, "user", query)
```

### Triggers

add a triggers block to your JSON schema
ex: update timestamp on set/update/create with a javascript expression
```yaml
triggers:
  set_timestamp:
    order: 1
    events:
      - on_create
      - on_update
      - on_set
    script: |
      doc.Set('timestamp', new Date().toISOString())
```

javascript variables are injected at runtime:
- `doc` - the JSON document that is being changed
- `db` - the global database instance(all methods are available)
- `ctx` - the context when the trigger was called
- `metadata` - the context metadata when the script is called
- `tx` - the current transaction instance

### Scripts

Scripts are javascript expressions/functions that can be called in Go - this may be used when embedded/dynamic functionality
is required.

getAccountScript:
```javascript
function setAccount(ctx, db, params) {
    db.tx(ctx, {isReadOnly: false}, (ctx, tx) => {
        tx.set(ctx, "account", params.doc)
    })
}
```

execute with custom paramaters:
```go
id := ksuid.New().String()
doc, err := myjson.NewDocumentFrom(map[string]any{
    "_id":  id,
    "name": gofakeit.Company(),
})
_, err = db.RunScript(ctx, "setAccount", getAccountScript, map[string]any{
    "doc": doc,
})
```
javascript variables are injected at runtime:
- `db` - the global database instance(all methods are available lowercased)
- `ctx` - the context when the script is called
- `metadata` - the context metadata when the script is called
- `newDocument` - function to intialize a new JSON document
- `newDocumentFrom` - function to initialize a new JSON document from a javascript object

## Tikv Setup Guide (full scale)

WIP - see [tikv foldeer](kv/tikv) w/ Makefile for running tikv locally

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
