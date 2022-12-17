package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"text/template"
	"time"

	_ "embed"

	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/gokvkit"
	"github.com/autom8ter/gokvkit/httpapi/api"
	"github.com/autom8ter/gokvkit/httpapi/handlers"
	"github.com/autom8ter/gokvkit/httpapi/middlewares"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
)

//go:embed templates/openapi.yaml.tmpl
var openapiTemplate string

// OpenAPIParams are custom params for generating an openapi specification
type OpenAPIParams struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description"`
}

type openAPIServer struct {
	db     *gokvkit.DB
	params *OpenAPIParams
	router chi.Router
}

// New creates a new openapi server
func New(db *gokvkit.DB, params *OpenAPIParams, mwares ...func(http.Handler) http.Handler) (api.OpenAPIServer, error) {
	if params == nil {
		return nil, fmt.Errorf("empty openapi params")
	}
	o := &openAPIServer{
		db:     db,
		params: params,
		router: chi.NewRouter(),
	}
	mwares = append([]func(http.Handler) http.Handler{middlewares.OpenAPIValidator(o)}, mwares...)
	o.router.Get("/openapi.yaml", handlers.SpecHandler(o))
	o.router.HandleFunc("/tx", handlers.TxHandler(o, websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}))
	o.router.Group(func(r chi.Router) {
		r.Use(mwares...)

		r.Post("/collections/{collection}", handlers.CreateDocHandler(o))

		r.Put("/collections/{collection}/{docID}", handlers.SetDocHandler(o))
		r.Patch("/collections/{collection}/{docID}", handlers.PatchDocHandler(o))
		r.Delete("/collections/{collection}/{docID}", handlers.DeleteDocHandler(o))
		r.Get("/collections/{collection}/{docID}", handlers.GetDocHandler(o))

		r.Post("/collections/{collection}/cmd/query", handlers.QueryHandler(o))
		r.Post("/collections/{collection}/cmd/batchset", handlers.BatchSetHandler(o))

		r.Get("/schema", handlers.GetSchemasHandler(o))
		r.Get("/schema/{collection}", handlers.GetSchemaHandler(o))
		r.Put("/schema/{collection}", handlers.PutSchemaHandler(o))

	})
	return o, nil
}

func (o *openAPIServer) DB() *gokvkit.DB {
	return o.db
}

func (o *openAPIServer) Spec() ([]byte, error) {
	t, err := template.New("").Funcs(sprig.FuncMap()).Parse(openapiTemplate)
	if err != nil {
		return nil, err
	}
	var coll []map[string]interface{}
	var collections = o.db.Collections()
	for _, c := range collections {
		schema, _ := o.db.GetSchema(c).Bytes()
		coll = append(coll, map[string]interface{}{
			"collection": c,
			"schema":     string(schema),
		})
	}
	buf := bytes.NewBuffer(nil)
	err = t.Execute(buf, map[string]any{
		"title":       o.params.Title,
		"description": o.params.Description,
		"version":     o.params.Version,
		"collections": coll,
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Handler returns the openapi http router
func (d *openAPIServer) Handler() http.Handler {
	return d.router
}

// Serve starts an http server serving openapi
func (d *openAPIServer) Serve(ctx context.Context, port int) error {
	egp, ctx := errgroup.WithContext(ctx)
	egp.Go(func() error {
		return http.ListenAndServe(fmt.Sprintf(":%v", port), d.Handler())
	})
	err := egp.Wait()
	d.shutdown()
	return err
}

func (d *openAPIServer) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	d.DB().Close(ctx)
}
