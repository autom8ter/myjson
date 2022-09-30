package wolverine

import (
	"context"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v3"
)

func (d *db) Set(ctx context.Context, record Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	record, err := record.Flatten()
	if err != nil {
		return err
	}
	if _, ok := d.collections[record.GetCollection()]; !ok {
		return fmt.Errorf("unsupported collection: %s", record.GetCollection())
	}
	collection := d.collections[record.GetCollection()]
	current, _ := d.Get(ctx, record.GetCollection(), record.GetID())
	if d.config.BeforeSet != nil {
		if err := d.config.BeforeSet(d, ctx, current, record); err != nil {
			return err
		}
	}
	bits, err := record.Encode()
	if err != nil {
		return err
	}
	if err := d.kv.Update(func(txn *badger.Txn) error {
		if err := txn.SetEntry(&badger.Entry{
			Key:       record.key(),
			Value:     bits,
			ExpiresAt: uint64(record.GetExpiresAt().Unix()),
		}); err != nil {
			return err
		}
		for _, index := range collection.Indexes {
			if err := txn.Delete(record.fieldIndexKey(index.Fields)); err != nil {
				return err
			}
			if err := txn.SetEntry(&badger.Entry{
				Key:       record.fieldIndexKey(index.Fields),
				Value:     bits,
				ExpiresAt: uint64(record.GetExpiresAt().Unix()),
			}); err != nil {
				return err
			}
		}
		if _, ok := d.fullText[record.GetCollection()]; ok {
			if err := d.fullText[record.GetCollection()].Index(record.GetID(), record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if d.config.AfterSet != nil {
		if err := d.config.AfterSet(d, ctx, current, record); err != nil {
			return err
		}
	}
	return nil
}

func (d *db) BatchSet(ctx context.Context, records []Record) error {
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return err
		}
		if _, ok := d.collections[record.GetCollection()]; !ok {
			return fmt.Errorf("unsupported collection: %s", record.GetCollection())
		}
	}
	if err := d.kv.Update(func(txn *badger.Txn) error {
		for _, record := range records {
			collection := d.collections[record.GetCollection()]
			record, err := record.Flatten()
			if err != nil {
				return err
			}
			current, _ := d.Get(ctx, record.GetCollection(), record.GetID())
			if d.config.BeforeSet != nil {
				if err := d.config.BeforeSet(d, ctx, current, record); err != nil {
					return err
				}
			}

			bits, err := record.Encode()
			if err != nil {
				return err
			}

			if err := txn.SetEntry(&badger.Entry{
				Key:       record.key(),
				Value:     bits,
				ExpiresAt: uint64(record.GetExpiresAt().Unix()),
			}); err != nil {
				return err
			}
			for _, index := range collection.Indexes {
				if err := txn.Delete(record.fieldIndexKey(index.Fields)); err != nil {
					return err
				}
				if err := txn.SetEntry(&badger.Entry{
					Key:       record.fieldIndexKey(index.Fields),
					Value:     bits,
					ExpiresAt: uint64(record.GetExpiresAt().Unix()),
				}); err != nil {
					return err
				}
			}
			if _, ok := d.fullText[record.GetCollection()]; ok {
				if err := d.fullText[record.GetCollection()].Index(record.GetID(), record); err != nil {
					return err
				}
			}
			if d.config.AfterSet != nil {
				if err := d.config.AfterSet(d, ctx, current, record); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *db) Update(ctx context.Context, record Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	collection, ok := d.collections[record.GetCollection()]
	if !ok {
		return fmt.Errorf("unsupported collection: %s/%s", record.GetCollection(), record.GetID())
	}
	record, err := record.Flatten()
	if err != nil {
		return err
	}
	current, err := d.Get(ctx, record.GetCollection(), record.GetID())
	if err != nil {
		return err
	}
	if d.config.BeforeUpdate != nil {
		if err := d.config.BeforeUpdate(d, ctx, current, record); err != nil {
			return err
		}
	}
	for k, v := range current {
		if _, ok := record[k]; !ok {
			record[k] = v
		}
	}
	bits, err := record.Encode()
	if err != nil {
		return err
	}
	if err := d.kv.Update(func(txn *badger.Txn) error {
		if err := txn.SetEntry(&badger.Entry{
			Key:       record.key(),
			Value:     bits,
			ExpiresAt: uint64(record.GetExpiresAt().Unix()),
		}); err != nil {
			return err
		}
		for _, index := range collection.Indexes {
			if err := txn.Delete(record.fieldIndexKey(index.Fields)); err != nil {
				return err
			}
			if err := txn.SetEntry(&badger.Entry{
				Key:       record.fieldIndexKey(index.Fields),
				Value:     bits,
				ExpiresAt: uint64(record.GetExpiresAt().Unix()),
			}); err != nil {
				return err
			}
		}
		if _, ok := d.fullText[record.GetCollection()]; ok {
			if err := d.fullText[record.GetCollection()].Index(record.GetID(), record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if d.config.AfterUpdate != nil {
		if err := d.config.AfterUpdate(d, ctx, current, record); err != nil {
			return err
		}
	}
	return nil
}

func (d *db) BatchUpdate(ctx context.Context, records []Record) error {
	for _, record := range records {
		if err := record.Validate(); err != nil {
			return err
		}
		if _, ok := d.collections[record.GetCollection()]; !ok {
			return fmt.Errorf("unsupported collection: %s/%s", record.GetCollection(), record.GetID())
		}
	}

	if err := d.kv.Update(func(txn *badger.Txn) error {
		for _, record := range records {
			collection := d.collections[record.GetCollection()]
			record, err := record.Flatten()
			if err != nil {
				return err
			}
			current, err := d.Get(ctx, record.GetCollection(), record.GetID())
			if err != nil {
				return err
			}
			for k, v := range current {
				if _, ok := record[k]; !ok {
					record[k] = v
				}
			}
			if d.config.BeforeUpdate != nil {
				if err := d.config.BeforeUpdate(d, ctx, current, record); err != nil {
					return err
				}
			}

			bits, err := record.Encode()
			if err != nil {
				return err
			}
			if err := txn.SetEntry(&badger.Entry{
				Key:       record.key(),
				Value:     bits,
				ExpiresAt: uint64(record.GetExpiresAt().Unix()),
			}); err != nil {
				return err
			}
			for _, index := range collection.Indexes {
				if err := txn.Delete(record.fieldIndexKey(index.Fields)); err != nil {
					return err
				}
				if err := txn.SetEntry(&badger.Entry{
					Key:       record.fieldIndexKey(index.Fields),
					Value:     bits,
					ExpiresAt: uint64(record.GetExpiresAt().Unix()),
				}); err != nil {
					return err
				}
			}
			if _, ok := d.fullText[record.GetCollection()]; ok {
				if err := d.fullText[record.GetCollection()].Index(record.GetID(), record); err != nil {
					return err
				}
			}
			if d.config.AfterUpdate != nil {
				if err := d.config.AfterUpdate(d, ctx, current, record); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *db) Delete(ctx context.Context, collection, id string) error {
	c, ok := d.collections[collection]
	if !ok {
		return fmt.Errorf("unsupported collection: %s/%s", collection, id)
	}
	record, err := d.Get(ctx, collection, id)
	if err != nil {
		return err
	}
	if d.config.BeforeDelete != nil {
		if err := d.config.BeforeDelete(d, ctx, record, nil); err != nil {
			return err
		}
	}
	if err := d.kv.Update(func(txn *badger.Txn) error {
		if err := txn.Delete([]byte(fmt.Sprintf("%s.%s", collection, id))); err != nil {
			return err
		}
		for _, index := range c.Indexes {
			if err := txn.Delete(record.fieldIndexKey(index.Fields)); err != nil {
				return err
			}
		}
		if _, ok := d.fullText[collection]; ok {
			if err := d.fullText[collection].Delete(id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if d.config.AfterDelete != nil {
		if err := d.config.AfterDelete(d, ctx, record, nil); err != nil {
			return err
		}
	}
	return nil
}

func (d *db) BatchDelete(ctx context.Context, collection string, ids []string) error {
	if _, ok := d.collections[collection]; !ok {
		return fmt.Errorf("unsupported collection: %s", collection)
	}
	if err := d.kv.Update(func(txn *badger.Txn) error {
		for _, id := range ids {
			c := d.collections[collection]
			record, err := d.Get(ctx, collection, id)
			if err != nil {
				return err
			}
			if d.config.BeforeDelete != nil {
				if err := d.config.BeforeDelete(d, ctx, record, nil); err != nil {
					return err
				}
			}
			if err := txn.Delete([]byte(fmt.Sprintf("%s.%s", collection, id))); err != nil {
				return err
			}
			for _, index := range c.Indexes {
				if err := txn.Delete(record.fieldIndexKey(index.Fields)); err != nil {
					return err
				}
			}
			if _, ok := d.fullText[collection]; ok {
				if err := d.fullText[collection].Delete(id); err != nil {
					return err
				}
			}
			if d.config.AfterDelete != nil {
				if err := d.config.AfterDelete(d, ctx, record, nil); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (d *db) DropAll(ctx context.Context, collections []string) error {
	for _, collection := range collections {
		if err := d.kv.DropPrefix([]byte(collection)); err != nil {
			return err
		}
		d.kv.DropPrefix([]byte(fmt.Sprintf("index.%s", collection)))
		if _, ok := d.fullText[collection]; ok {
			d.fullText[collection].Close()
			if err := os.RemoveAll(fmt.Sprintf("%s/search/%s.bleve", d.config.Path, collection)); err != nil && err != os.ErrNotExist {
				return err
			}
			delete(d.fullText, collection)
		}
	}
	return nil
}

func (d *db) QueryUpdate(ctx context.Context, update Record, collection string, query Query) error {
	records, err := d.Query(ctx, collection, query)
	if err != nil {
		return err
	}
	for _, record := range records {
		for k, v := range update {
			record[k] = v
		}
	}
	return d.BatchSet(ctx, records)
}

func (d *db) QueryDelete(ctx context.Context, collection string, query Query) error {
	records, err := d.Query(ctx, collection, query)
	if err != nil {
		return err
	}
	var ids []string
	for _, record := range records {
		ids = append(ids, record.GetID())
	}
	return d.BatchDelete(ctx, collection, ids)
}
