package wolverine

import (
	"context"
)

// CoreAPI is the core api powering database functionality
type CoreAPI interface {
	// Persist persists changes to a collection
	Persist(ctx context.Context, collection string, change StateChange) error
	// Scan scans the collection applying the scanner function to each matching document
	// it is less memory intensive that Query, which doesn't returns the full list of matching documents
	Scan(ctx context.Context, collection string, scan Scan, scanner ScanFunc) (IndexMatch, error)
	// ChangeStream streams state changes to the given function
	ChangeStream(ctx context.Context, collection string, fn ChangeStreamHandler) error
	// SetCollections sets the database's configured collections - it should be concurrency safe
	SetCollections(ctx context.Context, collections []*Collection) error
	// Close closes the database
	Close(ctx context.Context) error
}
