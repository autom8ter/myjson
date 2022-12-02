package gokvkit

import (
	"encoding/json"
	"github.com/autom8ter/gokvkit/kv"
	"net/http"
	"strings"
)

// Handler is an http handler that serves database commands and queries
func (db *DB) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.Split(r.URL.Path, "/")
		op := path[len(path)-1]
		collection := path[len(path)-2]
		if !db.hasCollection(collection) {
			http.Error(w, "collection does not exist", http.StatusBadRequest)
			return
		}
		md, _ := GetMetadata(r.Context())
		for k, v := range r.URL.Query() {
			md.Set(k, v[0])
		}
		for k, v := range r.Header {
			md.Set(k, v[0])
		}
		ctx := md.ToContext(r.Context())
		switch op {
		case "command":
			var commands []*Command
			if err := json.NewDecoder(r.Body).Decode(&commands); err != nil {
				http.Error(w, "failed to decode command", http.StatusBadRequest)
				return
			}
			err := db.kv.Tx(true, func(tx kv.Tx) error {
				return db.persistStateChange(ctx, tx, commands)
			})
			if err != nil {
				http.Error(w, "failed to persist commands", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(&commands)
		case "query":
			var q Query
			if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
				http.Error(w, "failed to decode query", http.StatusBadRequest)
				return
			}
			results, err := db.Query(ctx, q)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(&results)
		}
	})
}
