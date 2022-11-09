package brutus

import (
	"context"
)

// CoreAPI is the core api powering database functionality
type CoreAPI interface {
	// Persist persists changes to a collection and updates of it's indexes - it rolls back all changes if an error occurs
	Persist(ctx context.Context, collection string, change StateChange) error
	// Scan scans the collection applying the scanner function to each matching document
	// it is less memory intensive that Query, which doesn't returns the full list of matching documents
	Scan(ctx context.Context, collection string, scan Scan, scanner ScanFunc) (IndexMatch, error)
	// SetCollections sets the database's configured collections - it must be persistant and concurrency safe
	SetCollections(ctx context.Context, collections []*Collection) error
	// GetCollections gets the database's configured collections - it must be concurrency safe
	GetCollections(ctx context.Context) ([]*Collection, error)
	// GetCollection returns a single collection's configuration (if it exists)
	GetCollection(ctx context.Context, name string) (*Collection, bool)
	// Close closes the database
	Close(ctx context.Context) error
}
