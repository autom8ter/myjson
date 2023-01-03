# Tasks

an example http task microservice built with gokvkit

### Details
- 3 collections: account, user, task
- tasks belong to users and users belong to accounts (foreign keys)
- tasks & users have triggers to update their timestamps to now() on set/update/create
- the first time the server is run, 100 accounts, 1000 users, and 3000 tasks will be seeded
- http server configures the collections on startup
- http server supports:
  - getting collection schemas
  - creating collection documents
  - updating collection documents
  - setting collection documents
  - deleting collection documents
  - querying collection documents


Starting the HTTP Server:

`go run main.go`

```
registered collections: [account cdc migration task user]
registered endpoint: [GET] /collections
registered endpoint: [POST] /collections/{collection}/documents
registered endpoint: [GET PUT PATCH DELETE] /collections/{collection}/documents/{id}
registered endpoint: [POST] /collections/{collection}/query
starting http server on port :8080
```


GET all accounts:

```
curl --location --request POST 'http://localhost:8080/collections/account/query' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data-raw '{
  "select": [{"field": "*"}]
}'
```

GET users belonging to an account:

```
curl --location --request POST 'http://localhost:8080/collections/user/query' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data-raw '{
  "select": [{"field": "*"}],
  "where": [{"field": "account_id", "op": "eq", "value": "$account"}],
  "limit": 10
}'
```

GET tasks belonging to a user:

```
curl --location --request POST 'http://localhost:8080/collections/task/query' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data-raw '{
  "select": [{"field": "*"}],
  "where": [{"field": "user", "op": "eq", "value": "$user"}]
}'
```

GET tasks joined to users:

```
curl --location --request POST 'http://localhost:8080/collections/task/query' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data-raw '{
  "select": [{"field": "user"}, {"field": "content"}, {"field": "usr.contact.email"}],
  "join": [{
      "collection": "user",
      "on": [{
        "field": "_id",
        "op": "eq",
        "value": "$user"
      }],
      "as": "usr"
  }],
  "limit": 10
}'
```