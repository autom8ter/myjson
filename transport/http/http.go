package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/spf13/cast"

	"github.com/autom8ter/wolverine"
)

// Handler returns an http handler that serves the database as a REST API
// GET/PUT/PATCH/DELETE "/collections/{collection}/ref/{id}" (json object in request body)
// PUT/PATCH "/collections/{collection}/batch" (json object array in request body)
// GET "/collections/{collection}/query"?select={}&where.{field}.{op}={}&order_by={}&direction={}&limit={}
// GET "/collections/{collection}/search?select={}&search={}&where.{field}.{op}={}&order_by={}&direction={}&limit={}"
func Handler(db wolverine.DB) (http.Handler, error) {
	router := mux.NewRouter()
	singleDocument := "/collections/{collection}/ref/{id}"
	batchDocuments := "/collections/{collection}/batch"
	queryCollection := "/collections/{collection}/query"
	// GET record
	db.Debug(context.Background(), fmt.Sprintf("registered endpoint: GET %s", singleDocument), map[string]interface{}{})
	router.HandleFunc(singleDocument, func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		start := time.Now()
		result, err := db.Get(r.Context(), vars["collection"], vars["id"])
		if err != nil {
			db.Error(r.Context(), "failed to get record", err, map[string]interface{}{
				"request.path": r.URL.Path,
				"request.vars": vars,
				"collection":   vars["collection"],
				"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
			})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		db.Debug(r.Context(), "query executed", map[string]interface{}{
			"request.path": r.URL.Path,
			"request.vars": vars,
			"collection":   vars["collection"],
			"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
		})
		json.NewEncoder(w).Encode(&result)
	}).Methods(http.MethodGet)

	// SET record
	db.Debug(context.Background(), fmt.Sprintf("registered endpoint: PUT %s", singleDocument), map[string]interface{}{})
	router.HandleFunc(singleDocument, func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		start := time.Now()
		var replace wolverine.Document
		json.NewDecoder(r.Body).Decode(&replace)
		replace.SetID(vars["id"])
		replace.SetCollection(vars["collection"])
		err := db.Set(r.Context(), vars["collection"], &replace)
		if err != nil {
			db.Error(r.Context(), "failed to set record", err, map[string]interface{}{
				"request.path": r.URL.Path,
				"request.vars": vars,
				"collection":   vars["collection"],
				"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
				"record":       replace,
			})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		db.Debug(r.Context(), "query executed", map[string]interface{}{
			"request.path": r.URL.Path,
			"request.vars": vars,
			"collection":   vars["collection"],
			"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
		})
		json.NewEncoder(w).Encode(&replace)
	}).Methods(http.MethodPut)

	// UPDATE record
	db.Debug(context.Background(), fmt.Sprintf("registered endpoint: PATCH %s", singleDocument), map[string]interface{}{})
	router.HandleFunc(singleDocument, func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		start := time.Now()
		var edit wolverine.Document
		json.NewDecoder(r.Body).Decode(&edit)
		edit.SetID(vars["id"])
		edit.SetCollection(vars["collection"])
		err := db.Update(r.Context(), vars["collection"], &edit)
		if err != nil {
			db.Error(r.Context(), "failed to update record", err, map[string]interface{}{
				"request.path": r.URL.Path,
				"request.vars": vars,
				"collection":   vars["collection"],
				"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
				"record":       edit,
			})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		db.Debug(r.Context(), "query executed", map[string]interface{}{
			"request.path": r.URL.Path,
			"request.vars": vars,
			"collection":   vars["collection"],
			"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
		})
		json.NewEncoder(w).Encode(&edit)
	}).Methods(http.MethodPatch)

	// DELETE record
	db.Debug(context.Background(), fmt.Sprintf("registered endpoint: DELETE %s", singleDocument), map[string]interface{}{})
	router.HandleFunc(singleDocument, func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		start := time.Now()
		err := db.Delete(r.Context(), vars["collection"], vars["id"])
		if err != nil {
			db.Error(r.Context(), "failed to delete record", err, map[string]interface{}{
				"request.path": r.URL.Path,
				"request.vars": vars,
				"collection":   vars["collection"],
				"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
			})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		db.Debug(r.Context(), "query executed", map[string]interface{}{
			"request.path": r.URL.Path,
			"request.vars": vars,
			"collection":   vars["collection"],
			"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
		})
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodDelete)

	// BATCH SET record
	db.Debug(context.Background(), fmt.Sprintf("registered endpoint: PUT %s", batchDocuments), map[string]interface{}{})
	router.HandleFunc(batchDocuments, func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		start := time.Now()
		var replaceAll []*wolverine.Document
		json.NewDecoder(r.Body).Decode(&replaceAll)
		err := db.BatchSet(r.Context(), vars["collection"], replaceAll)
		if err != nil {
			db.Error(r.Context(), "failed to set record", err, map[string]interface{}{
				"request.path": r.URL.Path,
				"request.vars": vars,
				"collection":   vars["collection"],
				"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
			})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		db.Debug(r.Context(), "query executed", map[string]interface{}{
			"request.path": r.URL.Path,
			"request.vars": vars,
			"collection":   vars["collection"],
			"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
		})
		json.NewEncoder(w).Encode(&replaceAll)
	}).Methods(http.MethodPut)

	// BATCH UPDATE record
	db.Debug(context.Background(), fmt.Sprintf("registered endpoint: PATCH %s", batchDocuments), map[string]interface{}{})
	router.HandleFunc(batchDocuments, func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		start := time.Now()
		var editAll []*wolverine.Document
		json.NewDecoder(r.Body).Decode(&editAll)

		err := db.BatchUpdate(r.Context(), vars["collection"], editAll)
		if err != nil {
			db.Error(r.Context(), "failed to update record", err, map[string]interface{}{
				"request.path": r.URL.Path,
				"request.vars": vars,
				"collection":   vars["collection"],
				"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
			})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		db.Debug(r.Context(), "query executed", map[string]interface{}{
			"request.path": r.URL.Path,
			"request.vars": vars,
			"collection":   vars["collection"],
			"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
		})
		json.NewEncoder(w).Encode(&editAll)
	}).Methods(http.MethodPatch)

	// QUERY records
	db.Debug(context.Background(), fmt.Sprintf("registered endpoint: GET %s", queryCollection), map[string]interface{}{})
	router.HandleFunc(queryCollection, func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		var where []wolverine.Where
		for k, v := range r.URL.Query() {
			if strings.HasPrefix(k, "where.") {
				split := strings.Split(strings.TrimPrefix(k, "where."), ".")
				where = append(where, wolverine.Where{
					Field: split[0],
					Op:    wolverine.WhereOp(split[1]),
					Value: v[0],
				})
			}
		}
		query := wolverine.Query{
			Select:  strings.Split(r.URL.Query().Get("select"), ","),
			Where:   where,
			StartAt: cast.ToString(r.URL.Query().Get("start_at")),
			Limit:   cast.ToInt(r.URL.Query().Get("limit")),
			OrderBy: wolverine.OrderBy{
				Field:     r.URL.Query().Get("order_by"),
				Direction: wolverine.OrderByDirection(r.URL.Query().Get("direction")),
			},
		}
		start := time.Now()
		results, err := db.Query(r.Context(), vars["collection"], query)
		if err != nil {
			db.Error(r.Context(), "failed to query records", err, map[string]interface{}{
				"request.path": r.URL.Path,
				"request.vars": vars,
				"collection":   vars["collection"],
				"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
				"query":        query,
			})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		db.Debug(r.Context(), "query executed", map[string]interface{}{
			"request.path": r.URL.Path,
			"request.vars": vars,
			"collection":   vars["collection"],
			"query":        query,
			"duration":     float64(time.Since(start).Microseconds()) / float64(1000),
		})
		json.NewEncoder(w).Encode(&results)
	}).Methods(http.MethodGet)
	return router, nil
}
