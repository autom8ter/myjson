package wolverine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/pb"
)

func (d *db) Stream(ctx context.Context, collections []string, fn func(ctx context.Context, records []Record) error) error {
	var matches []pb.Match
	if len(collections) == 0 {
		matches = append(matches, pb.Match{Prefix: []byte("")})
	}
	for _, collection := range collections {
		matches = append(matches, pb.Match{
			Prefix:      []byte(fmt.Sprintf("%s.", collection)),
			IgnoreBytes: "",
		})
	}
	return d.kv.Subscribe(ctx, func(kv *badger.KVList) error {
		var records []Record
		for _, val := range kv.Kv {
			if bytes.HasPrefix(val.Key, []byte("index.")) {
				continue
			}
			data := Record{}
			if err := json.Unmarshal(val.Value, &data); err != nil {
				return err
			}
			if d.config.OnStream != nil {
				if d.config.OnStream != nil {
					if err := d.config.OnStream(d, ctx, data); err != nil {
						return err
					}
				}
			}
			records = append(records, data)
		}
		if err := fn(ctx, records); err != nil {
			return err
		}
		return nil
	}, matches)
}
