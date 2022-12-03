package gokvkit

import (
	"encoding/json"
	"github.com/autom8ter/gokvkit/kv"
	"github.com/palantir/stacktrace"
	"io"
	"net/http"
	"strings"
)

// Handler is an http handler that serves database commands and queries
func (db *DB) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.Split(r.URL.Path, "/")
		op := path[len(path)-1]
		collection := path[len(path)-2]
		handler := db.router.Get(collection, op, r.Method)
		if handler == nil {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "%s %s | zero operations for request", r.Method, r.RequestURI))
			return
		}
		md, _ := GetMetadata(r.Context())
		for k, v := range r.URL.Query() {
			md.Set(k, v[0])
		}
		for k, v := range r.Header {
			md.Set(k, v[0])
		}
		handler.ServeHTTP(w, r.WithContext(md.ToContext(r.Context())))
	})
}

func queryHandler(collection string, db *DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var q Query
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode query"))
			return
		}
		results, err := db.Query(r.Context(), q)
		if err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&results)
	})
}

func commandHandler(collection string, db *DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var commands []*Command
		if err := json.NewDecoder(r.Body).Decode(&commands); err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode command"))
			return
		}
		err := db.kv.Tx(true, func(tx kv.Tx) error {
			return db.persistStateChange(r.Context(), tx, commands)
		})
		if err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&commands)
	})
}

func schemaHandler(collection string, db *DB) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if !db.hasCollection(collection) {
				httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
				return
			}
			bits, _ := db.getCollectionSchema(collection)
			w.WriteHeader(http.StatusOK)
			w.Write(bits)
		case http.MethodPut:
			bits, err := io.ReadAll(r.Body)
			if err != nil {
				httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to read request body"))
				return
			}
			if err := db.ConfigureCollection(r.Context(), bits); err != nil {
				httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to configure collection"))
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	})
}

func httpError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if cde := stacktrace.GetCode(err); cde >= 400 && cde < 600 {
		status = int(cde)
	}
	http.Error(w, stacktrace.RootCause(err).Error(), status)
	return
}
