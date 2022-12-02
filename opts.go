package gokvkit

import "github.com/autom8ter/gokvkit/internal/safe"

// DBOpt is an option for configuring a collection
type DBOpt func(d *DB)

// WithOnRead adds document read hook(s) to the collection
func WithOnRead(readHooks map[string][]OnRead) DBOpt {
	return func(d *DB) {
		d.readHooks = safe.NewMap(readHooks)
	}
}

// WithOnPersist adds a hook to the collections configuration that executes on changes as commands are persisted
func WithOnPersist(sideEffects map[string][]OnPersist) DBOpt {
	return func(d *DB) {
		d.persistHooks = safe.NewMap(sideEffects)
	}
}

// WithOnWhere adds a wherre effect to the collections configuration that executes on on where clauses before queries are executed.
func WithOnWhere(whereHook map[string][]OnWhere) DBOpt {
	return func(d *DB) {
		d.whereHooks = safe.NewMap(whereHook)
	}
}

// WithOnInit adds database initializers(s) to the database.
func WithOnInit(inits map[string]OnInit) DBOpt {
	return func(d *DB) {
		d.initHooks = safe.NewMap(inits)
	}
}
