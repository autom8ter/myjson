package myjson

import (
	"context"

	"github.com/autom8ter/myjson/errors"
	"github.com/samber/lo"
)

func (t *transaction) authorizeCommand(ctx context.Context, schema CollectionSchema, command *persistCommand) (bool, error) {
	if isInternal(ctx) {
		return true, nil
	}
	if len(schema.Authz().Rules) == 0 {
		return true, nil
	}
	deny := lo.Filter(schema.Authz().Rules, func(a AuthzRule, i int) bool {
		if a.Action[0] == "*" && a.Effect == Deny {
			return true
		}
		return lo.Contains(a.Action, command.Action) && a.Effect == Deny
	})
	if len(deny) > 0 {
		for _, d := range deny {
			d.Match = t.db.globalScripts + d.Match
			result, err := t.vm.RunString(d.Match)
			if err != nil {
				return false, errors.Wrap(err, 0, "failed to run authz match script")
			}
			if result.ToBoolean() {
				return false, nil
			}
		}
	}
	allow := lo.Filter(schema.Authz().Rules, func(a AuthzRule, i int) bool {
		if a.Action[0] == "*" && a.Effect == Allow {
			return true
		}
		return lo.Contains(a.Action, command.Action) && a.Effect == Allow
	})
	if len(allow) == 0 {
		return true, nil
	}
	for _, d := range allow {
		d.Match = t.db.globalScripts + d.Match
		result, err := t.vm.RunString(d.Match)
		if err != nil {
			return false, errors.Wrap(err, 0, "failed to run authz match script")
		}
		if result.ToBoolean() {
			return true, nil
		}
	}
	return false, nil
}

func (t *transaction) authorizeQuery(ctx context.Context, schema CollectionSchema, query *Query) (bool, error) {
	if isInternal(ctx) || isIndexing(ctx) {
		return true, nil
	}
	if len(schema.Authz().Rules) == 0 {
		return true, nil
	}
	if err := t.vm.Set(string(JavascriptGlobalCtx), ctx); err != nil {
		return false, err
	}
	if err := t.vm.Set(string(JavascriptGlobalSchema), schema); err != nil {
		return false, err
	}
	if err := t.vm.Set(string(JavascriptGlobalQuery), *query); err != nil {
		return false, err
	}
	meta := ExtractMetadata(ctx)
	if err := t.vm.Set(string(JavascriptGlobalMeta), meta); err != nil {
		return false, err
	}

	deny := lo.Filter(schema.Authz().Rules, func(a AuthzRule, i int) bool {
		if a.Action[0] == "*" && a.Effect == Deny {
			return true
		}
		return lo.Contains(a.Action, QueryAction) && a.Effect == Deny
	})
	if len(deny) > 0 {
		for _, d := range deny {
			d.Match = t.db.globalScripts + d.Match
			result, err := t.vm.RunString(d.Match)
			if err != nil {
				return false, errors.Wrap(err, 0, "failed to run authz match script")
			}
			if result.ToBoolean() {
				return false, nil
			}
		}
	}
	allow := lo.Filter(schema.Authz().Rules, func(a AuthzRule, i int) bool {
		if a.Action[0] == "*" && a.Effect == Allow {
			return true
		}
		return lo.Contains(a.Action, QueryAction) && a.Effect == Allow
	})
	if len(allow) == 0 {
		return true, nil
	}
	for _, d := range allow {
		d.Match = t.db.globalScripts + d.Match
		result, err := t.vm.RunString(d.Match)
		if err != nil {
			return false, errors.Wrap(err, 0, "failed to run authz match script")
		}
		if result.ToBoolean() {
			return true, nil
		}
	}
	return false, nil
}

func (t *defaultDB) authorizeConfigure(ctx context.Context, schema CollectionSchema) (bool, error) {
	if isInternal(ctx) || isIndexing(ctx) {
		return true, nil
	}
	if len(schema.Authz().Rules) == 0 {
		return true, nil
	}
	vm := <-t.vmPool
	if err := vm.Set(string(JavascriptGlobalCtx), ctx); err != nil {
		return false, err
	}
	if err := vm.Set(string(JavascriptGlobalSchema), schema); err != nil {
		return false, err
	}
	meta := ExtractMetadata(ctx)
	if err := vm.Set(string(JavascriptGlobalMeta), meta); err != nil {
		return false, err
	}

	deny := lo.Filter(schema.Authz().Rules, func(a AuthzRule, i int) bool {
		if a.Action[0] == "*" && a.Effect == Deny {
			return true
		}
		return lo.Contains(a.Action, ConfigureAction) && a.Effect == Deny
	})
	if len(deny) > 0 {
		for _, d := range deny {
			d.Match = t.globalScripts + d.Match
			result, err := vm.RunString(d.Match)
			if err != nil {
				return false, errors.Wrap(err, 0, "failed to run authz match script")
			}
			if result.ToBoolean() {
				return false, nil
			}
		}
	}
	allow := lo.Filter(schema.Authz().Rules, func(a AuthzRule, i int) bool {
		if a.Action[0] == "*" && a.Effect == Allow {
			return true
		}
		return lo.Contains(a.Action, ConfigureAction) && a.Effect == Allow
	})
	if len(allow) == 0 {
		return true, nil
	}
	for _, d := range allow {
		d.Match = t.globalScripts + d.Match
		result, err := vm.RunString(d.Match)
		if err != nil {
			return false, errors.Wrap(err, 0, "failed to run authz match script")
		}
		if result.ToBoolean() {
			return true, nil
		}
	}
	return false, nil
}

func (t *defaultDB) authorizeChangeStream(ctx context.Context, schema CollectionSchema, filter []Where) (bool, error) {
	if isInternal(ctx) || isIndexing(ctx) {
		return true, nil
	}
	if len(schema.Authz().Rules) == 0 {
		return true, nil
	}
	vm := <-t.vmPool
	if err := vm.Set(string(JavascriptGlobalCtx), ctx); err != nil {
		return false, err
	}
	if err := vm.Set(string(JavascriptGlobalSchema), schema); err != nil {
		return false, err
	}
	if err := vm.Set(string(JavascriptGlobalFilter), filter); err != nil {
		return false, err
	}
	meta := ExtractMetadata(ctx)
	if err := vm.Set(string(JavascriptGlobalMeta), meta); err != nil {
		return false, err
	}

	deny := lo.Filter(schema.Authz().Rules, func(a AuthzRule, i int) bool {
		if a.Action[0] == "*" && a.Effect == Deny {
			return true
		}
		return lo.Contains(a.Action, ChangeStreamAction) && a.Effect == Deny
	})
	if len(deny) > 0 {
		for _, d := range deny {
			d.Match = t.globalScripts + d.Match
			result, err := vm.RunString(d.Match)
			if err != nil {
				return false, errors.Wrap(err, 0, "failed to run authz match script")
			}
			if result.ToBoolean() {
				return false, nil
			}
		}
	}
	allow := lo.Filter(schema.Authz().Rules, func(a AuthzRule, i int) bool {
		if a.Action[0] == "*" && a.Effect == Allow {
			return true
		}
		return lo.Contains(a.Action, ChangeStreamAction) && a.Effect == Allow
	})
	if len(allow) == 0 {
		return true, nil
	}
	for _, d := range allow {
		d.Match = t.globalScripts + d.Match
		result, err := vm.RunString(d.Match)
		if err != nil {
			return false, errors.Wrap(err, 0, "failed to run authz match script")
		}
		if result.ToBoolean() {
			return true, nil
		}
	}
	return false, nil
}
