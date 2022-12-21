package gokvkit

// DBOpt is an option for configuring a collection
type DBOpt func(d *DB)

// WithOnRead adds document read hook(s) to the collection
func WithOnRead(readHooks map[string][]OnRead) DBOpt {
	return func(d *DB) {
		d.readHooks = newInMemCache(readHooks)
	}
}

// WithOnPersist adds a hook to the collections configuration that executes on changes as commands are persisted
func WithOnPersist(sideEffects map[string][]OnPersist) DBOpt {
	return func(d *DB) {
		d.persistHooks = newInMemCache(sideEffects)
	}
}

// WithOnWhere adds a wherre effect to the collections configuration that executes on on where clauses before queries are executed.
func WithOnWhere(whereHook map[string][]OnWhere) DBOpt {
	return func(d *DB) {
		d.whereHooks = newInMemCache(whereHook)
	}
}

// WithOnInit adds database initializers(s) to the database.
func WithOnInit(inits map[string]OnInit) DBOpt {
	return func(d *DB) {
		d.initHooks = newInMemCache(inits)
	}
}

// WithOnCommit adds database hooks that execute before a transaction is commited
func WithOnCommit(onCommit ...OnCommit) DBOpt {
	return func(d *DB) {
		d.onCommit = append(d.onCommit, onCommit...)
	}
}

// WithOnRollback adds database hooks that execute before a transaction is rolled back
func WithOnRollback(onRollback ...OnRollback) DBOpt {
	return func(d *DB) {
		d.onRollback = append(d.onRollback, onRollback...)
	}
}

// WithOptimizer overrides the default query optimizer provider
func WithOptimizer(o Optimizer) DBOpt {
	return func(d *DB) {
		d.optimizer = o
	}
}

// WithCollectionCache overrides the default collection cache provider
func WithCollectionCache(c Cache[CollectionSchema]) DBOpt {
	return func(d *DB) {
		d.collections = c
	}
}

// WithChangeStream overrides the default change stream provider
func WithChangeStream(c Stream[CDC]) DBOpt {
	return func(d *DB) {
		d.cdcStream = c
	}
}
