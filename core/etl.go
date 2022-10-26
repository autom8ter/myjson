package core

import "context"

type Transformer func(ctx context.Context, docs []*Document) ([]*Document, error)

type ETL struct {
	OutputCollection string
	Query            Query
	Transformer      Transformer
}
