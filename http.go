package gokvkit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"

	_ "embed"

	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/autom8ter/gokvkit/model"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/go-chi/chi/v5"
	"github.com/palantir/stacktrace"
	"gopkg.in/yaml.v2"
)

//go:embed internal/templates/openapi.yaml.tmpl
var openapiTemplate string

type openAPIParams struct {
	title       string
	version     string
	description string
}

var defaultOpenAPIParams = openAPIParams{
	title:       "gokvkit API",
	version:     "0.0.0",
	description: "an API built with gokvkit",
}

func getOpenAPISpec(collections *safe.Map[*collectionSchema], params *openAPIParams) ([]byte, error) {
	if params == nil {
		params = &defaultOpenAPIParams
	}
	t, err := template.New("").Funcs(sprig.FuncMap()).Parse(openapiTemplate)
	if err != nil {
		return nil, err
	}
	var coll []map[string]interface{}
	collections.RangeR(func(key string, schema *collectionSchema) bool {
		coll = append(coll, map[string]interface{}{
			"collection": schema.collection,
			"schema":     string(schema.yamlRaw),
		})
		return true
	})
	buf := bytes.NewBuffer(nil)
	err = t.Execute(buf, map[string]any{
		"title":       params.title,
		"description": params.description,
		"version":     params.version,
		"collections": coll,
	})
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return buf.Bytes(), nil
}

func registerHTTPEndpoints(db *DB) {
	db.middlewares = append([]func(http.Handler) http.Handler{openAPIValidator(db), metadataInjector()}, db.middlewares...)
	db.router.Get("/openapi.yaml", specHandler(db))
	db.router.Group(func(r chi.Router) {
		r.Use(db.middlewares...)

		r.Post("/collections/{collection}", createDocHandler(db))

		r.Put("/collections/{collection}/{docID}", setDocHandler(db))
		r.Patch("/collections/{collection}/{docID}", patchDocHandler(db))
		r.Delete("/collections/{collection}/{docID}", deleteDocHandler(db))
		r.Get("/collections/{collection}/{docID}", getDocHandler(db))

		r.Post("/collections/{collection}/_/query", queryHandler(db))
		r.Post("/collections/{collection}/_/batch", batchSetHandler(db))

		r.Get("/schema", getSchemasHandler(db))
		r.Get("/schema/{collection}", getSchemaHandler(db))
		r.Put("/schema/{collection}", putSchemaHandler(db))
	})
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
		if err := db.setPrimaryKey(collection, doc, docID); err != nil {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "bad id: %s", docID))
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
		var resp = map[string]any{}
		db.collections.RangeR(func(key string, c *collectionSchema) bool {
			resp[key] = string(c.yamlRaw)
			return true
		})
		w.WriteHeader(http.StatusOK)
		yaml.NewEncoder(w).Encode(&resp)
	})
}

func getSchemaHandler(db *DB) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		collection := chi.URLParam(r, "collection")
		if !db.hasCollection(collection) {
			httpError(w, stacktrace.NewErrorWithCode(http.StatusBadRequest, "collection does not exist"))
			return
		}
		c, err := db.getPersistedCollection(collection)
		if err != nil {
			httpError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(c.yamlRaw)
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
		bits, _ := getOpenAPISpec(db.collections, db.openAPIParams)
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

func metadataInjector() func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			md, _ := model.GetMetadata(r.Context())
			for k, v := range r.Header {
				md.Set(fmt.Sprintf("http.header.%s", k), v)
			}
			handler.ServeHTTP(w, r.WithContext(md.ToContext(r.Context())))
		})
	}
}

func openAPIValidator(db *DB) func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bits, _ := getOpenAPISpec(db.collections, db.openAPIParams)
			loader := openapi3.NewLoader()
			doc, _ := loader.LoadFromData(bits)
			err := doc.Validate(loader.Context)
			if err != nil {
				httpError(w, stacktrace.PropagateWithCode(err, http.StatusInternalServerError, "invalid openapi spec"))
				return
			}
			router, err := gorillamux.NewRouter(doc)
			if err != nil {
				httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, "failed to configure collection"))
				return
			}
			md, _ := model.GetMetadata(r.Context())
			route, pathParams, err := router.FindRoute(r)
			if err != nil {
				httpError(w, stacktrace.PropagateWithCode(err, http.StatusNotFound, "route not found"))
				return
			}
			requestValidationInput := &openapi3filter.RequestValidationInput{
				Request:    r,
				PathParams: pathParams,
				Route:      route,
				Options: &openapi3filter.Options{AuthenticationFunc: func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
					return nil
				}},
			}

			if err := openapi3filter.ValidateRequest(r.Context(), requestValidationInput); err != nil {
				httpError(w, stacktrace.PropagateWithCode(err, http.StatusBadRequest, ""))
				return
			}
			md.SetAll(map[string]any{
				"openapi.path_params": pathParams,
				"openapi.route":       route.Path,
			})
			handler.ServeHTTP(w, r.WithContext(md.ToContext(r.Context())))
		})
	}
}
