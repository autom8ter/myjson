# Tasks

an example http server built with gokvkit

details:
- 3 collections: account, user, task
- tasks belong to users and users belong to accounts (foreign keys)
- tasks & users have triggers to update their timestamps to now() on set/update/create
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

