package gokvkit

import (
	"bytes"
	_ "embed"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/autom8ter/gokvkit/internal/safe"
	"github.com/autom8ter/gokvkit/model"
	"github.com/palantir/stacktrace"
)

//go:embed openapi.yaml.tmpl
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
		"querySchema": model.QuerySchema,
		"pageSchema":  model.PageSchema,
	})
	if err != nil {
		return nil, stacktrace.Propagate(err, "")
	}
	return buf.Bytes(), nil
}
