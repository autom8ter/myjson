package gokvkit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/autom8ter/gokvkit/model"
	"github.com/go-chi/chi/v5"
	"github.com/palantir/stacktrace"
)

// Handler is an http handler that serves database commands and queries
func (db *DB) Handler() http.Handler {
	mux := chi.NewRouter()
	mux.Get("/spec", specHandler(db))

	mux.Post("/collections/{collection}", createDocHandler(db))

	mux.Put("/collections/{collection}/{docID}", setDocHandler(db))
	mux.Patch("/collections/{collection}/{docID}", patchDocHandler(db))
	mux.Delete("/collections/{collection}/{docID}", deleteDocHandler(db))
	mux.Get("/collections/{collection}/{docID}", getDocHandler(db))

	mux.Post("/collections/{collection}/_/query", queryHandler(db))
	mux.Post("/collections/{collection}/_/batch", batchSetHandler(db))

	mux.Get("/schema", getSchemasHandler(db))
	mux.Get("/schema/{collection}", getSchemaHandler(db))
	mux.Put("/schema/{collection}", putSchemaHandler(db))

	return mux
}

func queryHandler(db *DB) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		var q model.Query
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode query"))
			return
		}
		results, err := db.Query(r.Context(), collection, q)
		if err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&results)
	})
}

func createDocHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		var doc = model.NewDocument()
		if err := json.NewDecoder(r.Body).Decode(doc); err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode query"))
			return
		}
		if err := db.Tx(r.Context(), func(ctx context.Context, tx Tx) error {
			id, err := tx.Create(ctx, collection, doc)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if err := db.setPrimaryKey(collection, doc, id); err != nil {
				return stacktrace.Propagate(err, "")
			}
			return nil
		}); err != nil {
			httpError(w, err)
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}

func patchDocHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		var update = map[string]any{}
		if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode edit"))
			return
		}
		if err := db.Tx(r.Context(), func(ctx context.Context, tx Tx) error {
			err := tx.Update(ctx, collection, docID, update)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			return nil
		}); err != nil {
			httpError(w, err)
			return
		}
		doc, err := db.Get(r.Context(), collection, docID)
		if err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to edit document"))
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}

func setDocHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		var doc = model.NewDocument()
		if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode update"))
			return
		}
		if err := db.Tx(r.Context(), func(ctx context.Context, tx Tx) error {
			err := tx.Set(ctx, collection, doc)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			return nil
		}); err != nil {
			httpError(w, err)
			return
		}
		doc, err := db.Get(r.Context(), collection, docID)
		if err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to edit document"))
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}

func getDocHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		doc, err := db.Get(r.Context(), collection, docID)
		if err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusNotFound, "failed to get document"))
			return
		}
		json.NewEncoder(w).Encode(doc)
	}
}

func deleteDocHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		docID := chi.URLParam(r, "docID")
		if err := db.Tx(r.Context(), func(ctx context.Context, tx Tx) error {
			err := tx.Delete(ctx, collection, docID)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			return nil
		}); err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func batchSetHandler(db *DB) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		var docs []*model.Document
		if err := json.NewDecoder(r.Body).Decode(&docs); err != nil {
			httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to decode query"))
			return
		}
		if err := db.Tx(r.Context(), func(ctx context.Context, tx Tx) error {
			for _, d := range docs {
				if err := tx.Set(ctx, collection, d); err != nil {
					return stacktrace.Propagate(err, "")
				}
			}
			return nil
		}); err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

func getSchemasHandler(db *DB) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		schemas, _ := db.getPersistedCollections()
		var resp = map[string]string{}
		schemas.RangeR(func(key string, c *collectionSchema) bool {
			resp[c.collection] = c.raw.Raw
			return true
		})
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&resp)
	})
}

func getSchemaHandler(db *DB) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		bits, _ := db.getCollectionSchema(collection)
		w.WriteHeader(http.StatusOK)
		w.Write(bits)
	})
}

func putSchemaHandler(db *DB) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	})
}

func specHandler(db *DB) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bits, _ := getOpenAPISpec(db.collections, &db.openAPIParams)
		w.WriteHeader(http.StatusOK)
		w.Write(bits)
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
