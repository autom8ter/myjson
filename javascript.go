package myjson

import (
	"context"
	"time"

	"github.com/dop251/goja"
	"github.com/google/uuid"
	"github.com/segmentio/ksuid"
)

func getJavascriptVM(ctx context.Context, db Database, overrides map[string]any) (*goja.Runtime, error) {
	vm := goja.New()
	vm.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
	if err := vm.Set("db", db); err != nil {
		return nil, err
	}
	if err := vm.Set("ctx", ctx); err != nil {
		return nil, err
	}
	if err := vm.Set("metadata", ExtractMetadata(ctx)); err != nil {
		return nil, err
	}
	for k, v := range builtins {
		if err := vm.Set(k, v); err != nil {
			return nil, err
		}
	}
	for k, v := range overrides {
		if err := vm.Set(k, v); err != nil {
			return nil, err
		}
	}
	return vm, nil
}

var builtins = map[string]any{
	"newDocumentFrom": NewDocumentFrom,
	"newDocument":     NewDocument,
	"ksuid": func() string {
		return ksuid.New().String()
	},
	"uuid": func() string {
		return uuid.New().String()
	},
	"now": time.Now,
}
