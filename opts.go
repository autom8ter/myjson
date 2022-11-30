package gokvkit

import "github.com/autom8ter/gokvkit/internal/safe"

// DBOpt is an option for configuring a collection
type DBOpt func(d *DB)

// WithValidatorHook adds  document validator(s) to the collection (see JSONSchema for an example)
func WithValidatorHooks(validators map[string][]ValidatorHook) DBOpt {
	return func(d *DB) {
		d.validators = safe.NewMap(validators)
	}
}

// WithReadHooks adds document read hook(s) to the collection (see JSONSchema for an example)
func WithReadHooks(readHooks map[string][]ReadHook) DBOpt {
	return func(d *DB) {
		d.readHooks = safe.NewMap(readHooks)
	}
}

// WithSideEffectBeforeHook adds a side effect to the collections configuration that executes on changes as documents are persisted
func WithSideEffects(sideEffects map[string][]SideEffectHook) DBOpt {
	return func(d *DB) {
		d.sideEffects = safe.NewMap(sideEffects)
	}
}

// WithWhereHook adds a wherre effect to the collections configuration that executes on on where clauses before queries are executed.
func WithWhereHook(whereHook map[string][]WhereHook) DBOpt {
	return func(d *DB) {
		d.whereHooks = safe.NewMap(whereHook)
	}
}
