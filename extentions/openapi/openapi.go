package openapi

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"
	"net/http"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/myjson"
	"github.com/autom8ter/myjson/util"
	"github.com/deepmap/oapi-codegen/pkg/codegen"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/ghodss/yaml"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

//go:embed openapi.yaml.tmpl
var openapiTemplate string

// Config are custom params for generating an openapi specification
type Config struct {
	Title        string   `json:"title" yaml:"title" validate:"required"`
	Version      string   `json:"version" yaml:"version" validate:"required"`
	Description  string   `json:"description" yaml:"description" validate:"required"`
	Port         int      `json:"port" yaml:"port" validate:"required"`
	AllowOrigins []string `json:"allowOrigins"`
	LogLevel     string   `json:"logLevel"`
}

type OpenAPIServer struct {
	params        Config
	router        *mux.Router
	upgrader      websocket.Upgrader
	spec          []byte
	specMu        sync.RWMutex
	openapiRouter routers.Router
	logger        Logger
}

// New creates a new openapi server
func New(params Config, opts ...Opt) (*OpenAPIServer, error) {
	if err := util.ValidateStruct(params); err != nil {
		return nil, err
	}
	cfg := zap.NewProductionConfig()
	switch params.LogLevel {
	case "error", "ERROR":
		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "warn", "WARN":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "info", "INFO":
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	default:
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	l, err := cfg.Build(
		zap.WithCaller(true),
		zap.AddCallerSkip(1),
	)
	if err != nil {
		return nil, err
	}
	o := &OpenAPIServer{
		params:   params,
		router:   mux.NewRouter(),
		upgrader: websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 1024},
		logger:   zapLogger{logger: l},
	}
	for _, opt := range opts {
		opt(o)
	}
	return o, nil
}

func getSpec(ctx context.Context, config Config, db myjson.Database) ([]byte, error) {
	t, err := template.New("").Funcs(sprig.FuncMap()).Parse(openapiTemplate)
	if err != nil {
		return nil, err
	}
	var coll []map[string]interface{}
	var collections = db.Collections(ctx)
	for _, c := range collections {
		schema := db.GetSchema(ctx, c)
		yamlSchema, _ := schema.MarshalYAML()
		createSchema, err := schema.MarshalJSON()
		if err != nil {
			return nil, err
		}
		required := cast.ToStringSlice(gjson.GetBytes(createSchema, "required").Value())
		if len(required) > 0 {
			index := lo.IndexOf(required, schema.PrimaryKey())
			if index != -1 {
				required = util.RemoveElement(index, required)
			}
		}
		createSchema, err = sjson.SetBytes(createSchema, "required", required)
		if err != nil {
			return nil, err
		}
		createSchema, err = yaml.JSONToYAML(createSchema)
		if err != nil {
			return nil, err
		}
		coll = append(coll, map[string]interface{}{
			"collection":    c,
			"schema":        string(yamlSchema),
			"create_schema": string(createSchema),
			"is_read_only":  schema.IsReadOnly(),
		})
	}
	buf := bytes.NewBuffer(nil)
	err = t.Execute(buf, map[string]any{
		"title":        config.Title,
		"description":  config.Description,
		"version":      config.Version,
		"query_schema": myjson.QuerySchema(),
		"page_schema":  myjson.PageSchema(),
		"collections":  coll,
	})
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (o *OpenAPIServer) RegisterRoutes(ctx context.Context, db myjson.Database) error {
	if err := o.refreshSpec(db); err != nil {
		return err
	}
	mwares := []mux.MiddlewareFunc{
		handlers.CORS(
			handlers.AllowedMethods([]string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}),
			handlers.AllowedOrigins(o.params.AllowOrigins),
			handlers.AllowedHeaders([]string{"Content-Type", "Authorization"}),
		),

		o.openAPIValidator(),
		o.loggerWare(),
		handlers.RecoveryHandler(),
	}
	o.router.Use(mwares...)
	o.router.HandleFunc("/openapi.yaml", o.specHandler()).Methods(http.MethodGet)
	o.router.HandleFunc("/openapi.json", o.specHandler()).Methods(http.MethodGet)
	o.router.HandleFunc("/api/sdk", o.getSDKHandler(db)).Methods(http.MethodGet)
	o.router.HandleFunc("/api/tx", o.txHandler(db))
	o.router.HandleFunc("/api/schema", o.getSchemasHandler(db)).Methods(http.MethodGet)
	o.router.HandleFunc("/api/schema/{collection}", o.getSchemaHandler(db)).Methods(http.MethodGet)
	o.router.HandleFunc("/api/schema/{collection}", o.putSchemaHandler(db)).Methods(http.MethodPut)
	o.router.HandleFunc("/api/collections/{collection}/docs", o.createDocHandler(db)).Methods(http.MethodPost)
	o.router.HandleFunc("/api/collections/{collection}/docs/{docID}", o.setDocHandler(db)).Methods(http.MethodPut)
	o.router.HandleFunc("/api/collections/{collection}/docs/{docID}", o.patchDocHandler(db)).Methods(http.MethodPatch)
	o.router.HandleFunc("/api/collections/{collection}/docs/{docID}", o.deleteDocHandler(db)).Methods(http.MethodDelete)
	o.router.HandleFunc("/api/collections/{collection}/docs/{docID}", o.getDocHandler(db)).Methods(http.MethodGet)
	o.router.HandleFunc("/api/collections/{collection}/query", o.queryHandler(db)).Methods(http.MethodPost)
	o.router.HandleFunc("/api/collections/{collection}/batch", o.batchSetHandler(db)).Methods(http.MethodPut)
	return nil
}

func (o *OpenAPIServer) refreshSpec(db myjson.Database) error {
	o.specMu.Lock()
	defer o.specMu.Unlock()
	spec, err := getSpec(context.Background(), o.params, db)
	if err != nil {
		return err
	}
	o.spec = spec
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromData(o.spec)
	if err != nil {
		return err
	}
	if err := doc.Validate(loader.Context); err != nil {
		return err
	}
	router, err := gorillamux.NewRouter(doc)
	if err != nil {
		return err
	}
	o.openapiRouter = router
	return nil
}

// Spec returns the openapi specification
func (o *OpenAPIServer) Spec(db myjson.Database) ([]byte, error) {
	o.specMu.RLock()
	defer o.specMu.RUnlock()
	if o.spec == nil {
		spec, err := getSpec(context.Background(), o.params, db)
		if err != nil {
			return nil, err
		}
		return spec, nil
	}
	return o.spec, nil
}

// GenerateSDK generates a go SDK based on the database API schema
func (oapi *OpenAPIServer) GenerateSDK(db myjson.Database, packageName string, w io.Writer) error {
	spec, err := oapi.Spec(db)
	if err != nil {
		fmt.Println("failed to get openapi spec: ", err.Error())
		return err
	}
	loader := openapi3.NewLoader()
	swaggerSpec, err := loader.LoadFromData(spec)
	if err != nil {
		return err
	}
	code, err := codegen.Generate(swaggerSpec, codegen.Configuration{
		PackageName: packageName,
		Generate: codegen.GenerateOptions{
			Client:       true,
			Models:       true,
			EmbeddedSpec: true,
		},
		Compatibility: codegen.CompatibilityOptions{},
		OutputOptions: codegen.OutputOptions{
			SkipFmt:            false,
			SkipPrune:          false,
			IncludeTags:        nil,
			ExcludeTags:        nil,
			UserTemplates:      nil,
			ExcludeSchemas:     nil,
			ResponseTypeSuffix: "",
			ClientTypeName:     "",
		},
		ImportMapping:     nil,
		AdditionalImports: nil,
	})
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(code))
	if err != nil {
		return err
	}
	return nil
}

// Serve starts an openapi http server serving the database
func (o *OpenAPIServer) Serve(ctx context.Context, db myjson.Database) error {
	defer o.logger.Sync(ctx)
	if err := o.RegisterRoutes(ctx, db); err != nil {
		return err
	}
	egp, ctx := errgroup.WithContext(ctx)
	egp.Go(func() error {
		return http.ListenAndServe(fmt.Sprintf(":%v", o.params.Port), o.router)
	})
	egp.Go(func() error {
		ticker := time.NewTimer(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if err := o.refreshSpec(db); err != nil {
					return err
				}
			}
		}
	})
	return egp.Wait()
}

// Logger returns the openapi logging instance
func (o *OpenAPIServer) Logger() Logger {
	return o.logger
}
