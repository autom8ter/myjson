package wolverine

import (
	"context"
	"github.com/autom8ter/machine/v4"
	"github.com/autom8ter/wolverine/internal/prefix"
	"github.com/autom8ter/wolverine/schema"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/go-multierror"
	"github.com/palantir/stacktrace"
	"github.com/reactivex/rxgo/v2"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"
)

type Collection struct {
	collection *schema.Collection
	kv         *badger.DB
	fullText   bleve.Index
	triggers   []schema.Trigger
	machine    machine.Machine
}

func (c *Collection) persistEvent(ctx context.Context, event *schema.Event) error {
	if len(event.Documents) == 0 {
		return nil
	}
	if len(event.Documents) == 1 {
		return c.saveDocument(ctx, event)
	}
	for _, document := range event.Documents {
		document.Set("_collection", event.Collection)
		if err := document.Validate(); err != nil {
			return stacktrace.Propagate(err, "")
		}
	}
	txn := c.kv.NewWriteBatch()
	var batch *bleve.Batch
	if c.collection.Indexing().HasSearchIndex() {
		batch = c.fullText.NewBatch()
	}
	for _, document := range event.Documents {
		current, _ := c.Get(ctx, document.GetID())
		if current == nil {
			current = schema.NewDocument()
		}
		for _, c := range c.triggers {
			if err := c(ctx, event.Action, schema.Before, current, document); err != nil {
				return stacktrace.Propagate(err, "trigger failure")
			}
		}
		var bits []byte
		switch event.Action {
		case schema.Set:
			valid, err := c.collection.Validate(document)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if !valid {
				return stacktrace.NewError("%s/%s document has invalid schema", c.collection.Collection(), document.GetID())
			}
			bits = document.Bytes()
		case schema.Update:
			currentClone := current.Clone()
			currentClone.Merge(document)
			valid, err := c.collection.Validate(currentClone)
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if !valid {
				return stacktrace.NewError("%s/%s document has invalid schema", c.collection.Collection(), currentClone.GetID())
			}
			bits = currentClone.Bytes()
		}

		switch event.Action {
		case schema.Set, schema.Update:
			pkey := prefix.PrimaryKey(c.collection.Collection(), document.GetID())
			if err := txn.SetEntry(&badger.Entry{
				Key:   []byte(pkey),
				Value: bits,
			}); err != nil {
				return stacktrace.Propagate(err, "failed to batch save documents")
			}
			for _, idx := range c.collection.Indexing().Query {
				pindex := idx.Prefix(event.Collection)
				if current != nil {
					if err := txn.Delete([]byte(pindex.GetPrefix(current.Value(), current.GetID()))); err != nil {
						return stacktrace.Propagate(err, "failed to batch save documents")
					}
				}
				i := pindex.GetPrefix(document.Value(), document.GetID())
				if err := txn.SetEntry(&badger.Entry{
					Key:   []byte(i),
					Value: bits,
				}); err != nil {
					return stacktrace.Propagate(err, "failed to batch save documents")
				}
			}
			if batch != nil {
				if err := batch.Index(document.GetID(), document.Value()); err != nil {
					return stacktrace.Propagate(err, "failed to batch save documents")
				}
			}
		case schema.Delete:
			for _, i := range c.collection.Indexing().Query {
				pindex := i.Prefix(event.Collection)
				if err := txn.Delete([]byte(pindex.GetPrefix(current.Value(), current.GetID()))); err != nil {
					return stacktrace.Propagate(err, "failed to batch delete documents")
				}
			}
			if err := txn.Delete([]byte(prefix.PrimaryKey(c.collection.Collection(), current.GetID()))); err != nil {
				return stacktrace.Propagate(err, "failed to batch delete documents")
			}
			if batch != nil {
				batch.Delete(document.GetID())
			}
		}
		for _, t := range c.triggers {
			if err := t(ctx, event.Action, schema.After, current, document); err != nil {
				return stacktrace.Propagate(err, "trigger failure")
			}
		}
		for _, agg := range c.collection.Indexing().Aggregate {
			if err := agg.Trigger()(ctx, event.Action, schema.After, current, document); err != nil {
				return stacktrace.Propagate(err, "trigger failure")
			}
		}
	}
	if batch != nil {
		if err := c.fullText.Batch(batch); err != nil {
			return stacktrace.Propagate(err, "failed to batch documents")
		}
	}
	if err := txn.Flush(); err != nil {
		return stacktrace.Propagate(err, "failed to batch documents")
	}
	c.machine.Publish(ctx, machine.Message{
		Channel: event.Collection,
		Body:    event,
	})
	return nil
}

func (c *Collection) saveDocument(ctx context.Context, event *schema.Event) error {
	if len(event.Documents) == 0 {
		return nil
	}
	if len(event.Documents) > 1 {
		return c.persistEvent(ctx, event)
	}
	document := event.Documents[0]
	if err := document.Validate(); err != nil {
		return stacktrace.Propagate(err, "")
	}
	document.Set("_collection", event.Collection)
	current, _ := c.Get(ctx, document.GetID())
	if current == nil {
		current = schema.NewDocument()
	}
	for _, t := range c.triggers {
		if err := t(ctx, event.Action, schema.Before, current, document); err != nil {
			return stacktrace.Propagate(err, "trigger failure")
		}
	}
	var bits []byte
	switch event.Action {
	case schema.Set:
		valid, err := c.collection.Validate(document)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		if !valid {
			return stacktrace.NewError("%s/%s document has invalid schema", c.collection.Collection(), document.GetID())
		}
		bits = document.Bytes()
	case schema.Update:
		currentClone := current.Clone()
		currentClone.Merge(document)
		valid, err := c.collection.Validate(currentClone)
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		if !valid {
			return stacktrace.NewError("%s/%s document has invalid schema", c.collection.Collection(), document.GetID())
		}
		bits = currentClone.Bytes()
	}

	return c.kv.Update(func(txn *badger.Txn) error {
		switch event.Action {
		case schema.Set, schema.Update:
			if err := txn.SetEntry(&badger.Entry{
				Key:   []byte(prefix.PrimaryKey(c.collection.Collection(), document.GetID())),
				Value: bits,
			}); err != nil {
				return stacktrace.Propagate(err, "failed to save document")
			}
			for _, index := range c.collection.Indexing().Query {
				pindex := index.Prefix(event.Collection)
				if current != nil {
					if err := txn.Delete([]byte(pindex.GetPrefix(current.Value(), current.GetID()))); err != nil {
						return stacktrace.Propagate(err, "failed to save document")
					}
				}
				i := pindex.GetPrefix(document.Value(), document.GetID())
				if err := txn.SetEntry(&badger.Entry{
					Key:   []byte(i),
					Value: bits,
				}); err != nil {
					return stacktrace.Propagate(err, "failed to save document")
				}
			}
			if c.collection.Indexing().HasSearchIndex() {
				if err := c.fullText.Index(document.GetID(), document.Value()); err != nil {
					return stacktrace.Propagate(err, "failed to save document")
				}
			}
		case schema.Delete:
			for _, index := range c.collection.Indexing().Query {
				pindex := index.Prefix(event.Collection)
				if err := txn.Delete([]byte(pindex.GetPrefix(current.Value(), current.GetID()))); err != nil {
					return stacktrace.Propagate(err, "failed to delete document")
				}
			}
			if err := txn.Delete(prefix.PrimaryKey(c.collection.Collection(), current.GetID())); err != nil {
				return stacktrace.Propagate(err, "failed to delete document")
			}
			if c.collection.Indexing().HasSearchIndex() {
				if err := c.fullText.Delete(document.GetID()); err != nil {
					return stacktrace.Propagate(err, "failed to delete document")
				}
			}
		}
		for _, t := range c.triggers {
			if err := t(ctx, event.Action, schema.After, current, document); err != nil {
				return stacktrace.Propagate(err, "trigger failure")
			}
		}
		for _, agg := range c.collection.Indexing().Aggregate {
			if err := agg.Trigger()(ctx, event.Action, schema.After, current, document); err != nil {
				return stacktrace.Propagate(err, "trigger failure")
			}
		}
		c.machine.Publish(ctx, machine.Message{
			Channel: event.Collection,
			Body:    event,
		})
		return nil
	})
}

func (c *Collection) Query(ctx context.Context, query schema.Query) (schema.Page, error) {
	now := time.Now()
	qmachine := machine.New()
	index, err := c.collection.OptimizeQueryIndex(query.Where, query.OrderBy)
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "")
	}
	var (
		input = make(chan rxgo.Item)
	)
	qmachine.Go(ctx, func(ctx context.Context) error {
		if err := c.kv.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			opts.PrefetchSize = 10
			opts.Prefix = index.Ref.GetPrefix(schema.IndexableFields(query.Where, query.OrderBy), "")
			seek := opts.Prefix

			if query.OrderBy.Direction == schema.DESC {
				opts.Reverse = true
				seek = prefix.PrefixNextKey(opts.Prefix)
			}
			it := txn.NewIterator(opts)
			it.Seek(seek)
			defer it.Close()
			for it.ValidForPrefix(opts.Prefix) {
				if ctx.Err() != nil {
					return nil
				}
				item := it.Item()
				err := item.Value(func(bits []byte) error {
					document, err := schema.NewDocumentFromBytes(bits)
					if err != nil {
						return stacktrace.Propagate(err, "")
					}
					input <- rxgo.Of(document)
					return nil
				})
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				it.Next()
			}
			return nil
		}); err != nil {
			close(input)
			return stacktrace.Propagate(err, "")
		}
		close(input)
		return nil
	})
	var results []*schema.Document
	for result := range query.Observe(ctx, input, index.FullScan()).Observe() {
		doc, ok := result.V.(*schema.Document)
		if !ok {
			return schema.Page{}, stacktrace.NewError("expected type: %T got: %#v", &schema.Document{}, result.V)
		}
		results = append(results, doc)
	}
	results = schema.SortOrder(query.OrderBy, results)
	return schema.Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: schema.PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    index,
		},
	}, nil
}

func (c *Collection) Get(ctx context.Context, id string) (*schema.Document, error) {
	var (
		document *schema.Document
	)

	if err := c.kv.View(func(txn *badger.Txn) error {
		item, err := txn.Get(prefix.PrimaryKey(c.collection.Collection(), id))
		if err != nil {
			return stacktrace.Propagate(err, "")
		}
		return item.Value(func(val []byte) error {
			document, err = schema.NewDocumentFromBytes(val)
			return stacktrace.Propagate(err, "")
		})
	}); err != nil {
		return document, err
	}
	return document, nil
}

func (c *Collection) GetAll(ctx context.Context, ids []string) ([]*schema.Document, error) {
	var documents []*schema.Document
	if err := c.kv.View(func(txn *badger.Txn) error {
		for _, id := range ids {
			item, err := txn.Get([]byte(prefix.PrimaryKey(c.collection.Collection(), id)))
			if err != nil {
				return stacktrace.Propagate(err, "")
			}
			if err := item.Value(func(val []byte) error {
				document, err := schema.NewDocumentFromBytes(val)
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				documents = append(documents, document)
				return nil
			}); err != nil {
				return stacktrace.Propagate(err, "")
			}
		}
		return nil
	}); err != nil {
		return documents, err
	}
	return documents, nil
}

// QueryPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
func (c *Collection) QueryPaginate(ctx context.Context, query schema.Query, handlePage schema.PageHandler) error {
	page := query.Page
	for {
		results, err := c.Query(ctx, schema.Query{
			Select:  query.Select,
			Where:   query.Where,
			Page:    page,
			Limit:   query.Limit,
			OrderBy: query.OrderBy,
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to query collection: %s", c.collection.Collection())
		}
		if len(results.Documents) == 0 {
			return nil
		}
		if !handlePage(results) {
			return nil
		}
		page++
	}
}

func (c *Collection) ChangeStream(ctx context.Context, fn schema.ChangeStreamHandler) error {
	return c.machine.Subscribe(ctx, c.collection.Collection(), func(ctx context.Context, msg machine.Message) (bool, error) {
		switch event := msg.Body.(type) {
		case *schema.Event:
			if err := fn(ctx, event); err != nil {
				return false, stacktrace.Propagate(err, "")
			}
		case schema.Event:
			if err := fn(ctx, &event); err != nil {
				return false, stacktrace.Propagate(err, "")
			}
		}
		return true, nil
	})
}

func (c *Collection) Set(ctx context.Context, document *schema.Document) error {
	return stacktrace.Propagate(c.saveDocument(ctx, &schema.Event{
		Collection: c.collection.Collection(),
		Action:     schema.Set,
		Documents:  []*schema.Document{document},
	}), "")
}

func (c *Collection) BatchSet(ctx context.Context, batch []*schema.Document) error {
	return stacktrace.Propagate(c.persistEvent(ctx, &schema.Event{
		Collection: c.collection.Collection(),
		Action:     schema.Set,
		Documents:  batch,
	}), "")
}

func (c *Collection) Update(ctx context.Context, document *schema.Document) error {
	return stacktrace.Propagate(c.saveDocument(ctx, &schema.Event{
		Collection: c.collection.Collection(),
		Action:     schema.Update,
		Documents:  []*schema.Document{document},
	}), "")
}

func (c *Collection) BatchUpdate(ctx context.Context, batch []*schema.Document) error {
	return c.persistEvent(ctx, &schema.Event{
		Collection: c.collection.Collection(),
		Action:     schema.Update,
		Documents:  batch,
	})
}

func (c *Collection) Delete(ctx context.Context, id string) error {
	doc, err := c.Get(ctx, id)
	if err != nil {
		return stacktrace.Propagate(err, "failed to delete %s/%s", c.collection.Collection(), id)
	}
	return c.saveDocument(ctx, &schema.Event{
		Collection: c.collection.Collection(),
		Action:     schema.Delete,
		Documents:  []*schema.Document{doc},
	})
}

func (c *Collection) BatchDelete(ctx context.Context, ids []string) error {
	var documents []*schema.Document
	for _, id := range ids {
		doc, err := c.Get(ctx, id)
		if err != nil {
			return stacktrace.Propagate(err, "failed to batch delete %s/%s", c.collection.Collection(), id)
		}
		documents = append(documents, doc)
	}

	return c.persistEvent(ctx, &schema.Event{
		Collection: c.collection.Collection(),
		Action:     schema.Delete,
		Documents:  documents,
	})
}

func (c *Collection) QueryUpdate(ctx context.Context, update *schema.Document, query schema.Query) error {
	results, err := c.Query(ctx, query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	for _, document := range results.Documents {
		document.Merge(update)
	}
	return stacktrace.Propagate(c.BatchSet(ctx, results.Documents), "")
}

func (c *Collection) QueryDelete(ctx context.Context, query schema.Query) error {
	results, err := c.Query(ctx, query)
	if err != nil {
		return stacktrace.Propagate(err, "")
	}
	var ids []string
	for _, document := range results.Documents {
		ids = append(ids, document.GetID())
	}
	return stacktrace.Propagate(c.BatchDelete(ctx, ids), "")
}

func (c *Collection) aggregateIndex(ctx context.Context, i *schema.AggregateIndex, query schema.AggregateQuery) (schema.Page, error) {
	now := time.Now()
	input := make(chan rxgo.Item)
	go func() {
		results := i.Aggregate(query.Aggregates...)
		results = schema.SortOrder(query.OrderBy, results)
		for _, result := range results {
			input <- rxgo.Of(result)
		}
		close(input)
	}()
	limit := 1000000
	if query.Limit > 0 {
		limit = query.Limit
	}
	pipe := rxgo.FromChannel(input, rxgo.WithContext(ctx), rxgo.WithCPUPool(), rxgo.WithObservationStrategy(rxgo.Eager)).
		Skip(uint(query.Page * limit)).
		Take(uint(limit))
	var results []*schema.Document
	for result := range pipe.Observe() {
		doc, ok := result.V.(*schema.Document)
		if !ok {
			return schema.Page{}, stacktrace.NewError("expected type: %T got: %#v", &schema.Document{}, result.V)
		}
		results = append(results, doc)
	}

	return schema.Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: schema.PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    schema.QueryIndexMatch{},
		},
	}, nil
}

func (c *Collection) Aggregate(ctx context.Context, query schema.AggregateQuery) (schema.Page, error) {
	indexes := c.collection.Indexing()
	if indexes.Aggregate != nil {
		for _, i := range indexes.Aggregate {
			if i.Matches(query) {
				return c.aggregateIndex(ctx, i, query)
			}
		}
	}

	now := time.Now()
	var (
		input = make(chan rxgo.Item)
	)
	index, err := c.collection.OptimizeQueryIndex(query.Where, query.OrderBy)
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "")
	}

	go func() {
		if err := c.kv.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			opts.PrefetchSize = 10
			opts.Prefix = index.Ref.GetPrefix(schema.IndexableFields(query.Where, query.OrderBy), "")
			it := txn.NewIterator(opts)
			it.Seek(opts.Prefix)
			defer it.Close()
			for it.ValidForPrefix(opts.Prefix) {
				if ctx.Err() != nil {
					return nil
				}
				item := it.Item()
				err := item.Value(func(bits []byte) error {
					document, err := schema.NewDocumentFromBytes(bits)
					if err != nil {
						return stacktrace.Propagate(err, "")
					}
					input <- rxgo.Of(document)
					return nil
				})
				if err != nil {
					return stacktrace.Propagate(err, "")
				}
				it.Next()
			}
			return nil
		}); err != nil {
			close(input)
			panic(err)
		}
		close(input)
	}()

	pipe, err := query.Observe(ctx, input, index.FullScan())
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "")
	}
	var results []*schema.Document
	for result := range pipe.Observe() {
		doc, ok := result.V.(*schema.Document)
		if !ok {
			return schema.Page{}, nil
		}
		results = append(results, doc)
	}
	results = schema.SortOrder(query.OrderBy, results)
	return schema.Page{
		Documents: results,
		NextPage:  query.Page + 1,
		Count:     len(results),
		Stats: schema.PageStats{
			ExecutionTime: time.Since(now),
			IndexMatch:    index,
		},
	}, nil
}

func (c *Collection) Search(ctx context.Context, q schema.SearchQuery) (schema.Page, error) {
	now := time.Now()
	var (
		fields []string
		limit  = q.Limit
	)
	for _, w := range q.Where {
		fields = append(fields, w.Field)
	}
	if limit == 0 {
		limit = 1000
	}
	var queries []query.Query
	for _, where := range q.Where {
		if where.Value == nil {
			return schema.Page{}, stacktrace.NewError("empty where clause value")
		}
		switch where.Op {
		case schema.Basic:
			switch where.Value.(type) {
			case bool:
				qry := bleve.NewBoolFieldQuery(cast.ToBool(where.Value))
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			case float64, int, int32, int64, float32, uint64, uint, uint8, uint16, uint32:
				qry := bleve.NewNumericRangeQuery(lo.ToPtr(cast.ToFloat64(where.Value)), nil)
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			default:
				qry := bleve.NewMatchQuery(cast.ToString(where.Value))
				if where.Boost > 0 {
					qry.SetBoost(where.Boost)
				}
				qry.SetField(where.Field)
				queries = append(queries, qry)
			}
		case schema.DateRange:
			var (
				from time.Time
				to   time.Time
			)
			split := strings.Split(cast.ToString(where.Value), ",")
			from = cast.ToTime(split[0])
			if len(split) == 2 {
				to = cast.ToTime(split[1])
			}
			qry := bleve.NewDateRangeQuery(from, to)
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.TermRange:
			var (
				from string
				to   string
			)
			split := strings.Split(cast.ToString(where.Value), ",")
			from = split[0]
			if len(split) == 2 {
				to = split[1]
			}
			qry := bleve.NewTermRangeQuery(from, to)
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.GeoDistance:
			var (
				from     float64
				to       float64
				distance string
			)
			split := strings.Split(cast.ToString(where.Value), ",")
			if len(split) < 3 {
				return schema.Page{}, stacktrace.NewError("geo distance where clause requires 3 comma separated values: lat(float), lng(float), distance(string)")
			}
			from = cast.ToFloat64(split[0])
			to = cast.ToFloat64(split[1])
			distance = cast.ToString(split[2])
			qry := bleve.NewGeoDistanceQuery(from, to, distance)
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.Prefix:
			qry := bleve.NewPrefixQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.Fuzzy:
			qry := bleve.NewFuzzyQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.Regex:
			qry := bleve.NewRegexpQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		case schema.Wildcard:
			qry := bleve.NewWildcardQuery(cast.ToString(where.Value))
			if where.Boost > 0 {
				qry.SetBoost(where.Boost)
			}
			qry.SetField(where.Field)
			queries = append(queries, qry)
		}
	}
	if len(queries) == 0 {
		queries = []query.Query{bleve.NewMatchAllQuery()}
	}
	var searchRequest *bleve.SearchRequest
	if len(queries) > 1 {
		searchRequest = bleve.NewSearchRequestOptions(bleve.NewConjunctionQuery(queries...), q.Limit, q.Page*q.Limit, false)
	} else {
		searchRequest = bleve.NewSearchRequestOptions(bleve.NewConjunctionQuery(queries[0]), q.Limit, q.Page*q.Limit, false)
	}
	searchRequest.Fields = []string{"*"}
	results, err := c.fullText.Search(searchRequest)
	if err != nil {
		return schema.Page{}, stacktrace.Propagate(err, "failed to search index: %s", c.collection.Collection())
	}

	var data []*schema.Document
	for _, h := range results.Hits {
		if len(h.Fields) == 0 {
			continue
		}
		record, err := schema.NewDocumentFromMap(h.Fields)
		if err != nil {
			return schema.Page{}, stacktrace.Propagate(err, "failed to search index: %s", c.collection.Collection())
		}
		data = append(data, record)
	}
	if len(q.Select) > 0 {
		for _, r := range data {
			r.Select(q.Select)
		}
	}
	return schema.Page{
		Documents: data,
		NextPage:  q.Page + 1,
		Count:     len(data),
		Stats: schema.PageStats{
			ExecutionTime: time.Since(now),
		},
	}, nil
}

// SearchPaginate paginates through each page of the query until the handlePage function returns false or there are no more results
func (c *Collection) SearchPaginate(ctx context.Context, query schema.SearchQuery, handlePage schema.PageHandler) error {
	page := query.Page
	for {
		results, err := c.Search(ctx, schema.SearchQuery{
			Select: query.Select,
			Where:  query.Where,
			Page:   page,
			Limit:  query.Limit,
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to query collection: %s", c.collection.Collection())
		}
		if len(results.Documents) == 0 {
			return nil
		}
		if !handlePage(results) {
			return nil
		}
		page = results.NextPage
	}
}

// Reindex the collection
func (c *Collection) Reindex(ctx context.Context) error {
	egp, _ := errgroup.WithContext(ctx)
	var page int
	for {

		results, err := c.Query(ctx, schema.Query{
			Select:  nil,
			Where:   nil,
			Page:    page,
			Limit:   1000,
			OrderBy: schema.OrderBy{},
		})
		if err != nil {
			return stacktrace.Propagate(err, "failed to reindex collection: %s", c.collection.Collection())
		}
		if len(results.Documents) == 0 {
			break
		}
		var toSet []*schema.Document
		var toDelete []string
		for _, r := range results.Documents {
			result, _ := c.Get(ctx, r.GetID())
			if result != nil {
				toSet = append(toSet, result)
			} else {
				toDelete = append(toDelete, r.GetID())
				_ = c.Delete(ctx, r.GetID())
			}
		}
		if len(toSet) > 0 {
			egp.Go(func() error {
				return stacktrace.Propagate(c.BatchSet(ctx, toSet), "")
			})
		}
		if len(toDelete) > 0 {
			egp.Go(func() error {
				return stacktrace.Propagate(c.BatchDelete(ctx, toDelete), "")
			})
		}
		page = results.NextPage
	}
	if err := egp.Wait(); err != nil {
		return stacktrace.Propagate(err, "failed to reindex collection: %s", c.collection.Collection())
	}
	return nil
}

func (c *Collection) Close(ctx context.Context) error {
	err := c.machine.Wait()
	err = multierror.Append(err, c.fullText.Close())
	err = multierror.Append(err, c.kv.Sync())
	err = multierror.Append(err, c.kv.Close())
	if err, ok := err.(*multierror.Error); ok && len(err.Errors) > 0 {
		return stacktrace.Propagate(err, "database close failure")
	}
	return nil
}
