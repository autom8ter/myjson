package wolverine

import "context"

type ETLFunc func(ctx context.Context, docs Documents) (Documents, error)

type ETL struct {
	OutputCollection string
	Query            Query
}
