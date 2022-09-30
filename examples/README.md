# Examples


## Task/TODO API

run:
    
    go run examples/todo-api/main.go

query done tasks: 

    curl http://localhost:8080/collections/task/query?limit=50&order_by=account_id&direction=ASC&select=account_id,owner,done&where.done.eq=true

search tasks(full text):

    http://localhost:8080/tasks/search?fields=content&search=error&type=prefix&select=content&limit=25
