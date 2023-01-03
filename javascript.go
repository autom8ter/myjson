package gokvkit

import (
	"context"

	"github.com/dop251/goja"
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
	if err := vm.Set("newDocumentFrom", NewDocumentFrom); err != nil {
		return nil, err
	}
	if err := vm.Set("newDocument", NewDocument); err != nil {
		return nil, err
	}
	if err := vm.Set("ksuid", newID); err != nil {
		return nil, err
	}
	if overrides != nil {
		for k, v := range overrides {
			if err := vm.Set(k, v); err != nil {
				return nil, err
			}
		}
	}
	return vm, nil
}

func newID() string {
	return ksuid.New().String()
}
