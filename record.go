package wolverine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nqd/flat"
	"github.com/spf13/cast"
)

// Record is a database record with special attributes.
// required attributes: _id(string), _collection(string)
// optional attributes: _metadata(map[string]string), _expires_at(time.Time)
type Record map[string]interface{}

// Pointer is a pointer to a record - it only contains the _id & _collection of the record for lookup
type Pointer map[string]interface{}

// GetCollection gets the collection from the record pointer
func (p Pointer) GetCollection() string {
	return cast.ToString(p["_collection"])
}

// GetID gets the id from the record pointer
func (p Pointer) GetID() string {
	return cast.ToString(p["_id"])
}

func (p Pointer) key() []byte {
	return []byte(fmt.Sprintf("%s.%s", p.GetCollection(), p.GetID()))
}

// Encode marshals the record pointer to json
func (p Pointer) Encode() ([]byte, error) {
	return json.Marshal(&p)
}

// NewRecordFromJSON creates a new record from the provided json
func NewRecordFromJSON(data []byte) (Record, error) {
	r := Record{}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return r.Flatten()
}

// NewRecordFromStruct creates a new record from the provided struct, collection, and id
func NewRecordFromStruct(collection, id string, value any) (Record, error) {
	bits, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	r, err := NewRecordFromJSON(bits)
	if err != nil {
		return nil, err
	}
	r.SetCollection(collection)
	r.SetID(id)
	return r, nil
}

// String returns the record as a json string
func (r Record) String() string {
	bits, _ := r.Encode()
	return string(bits)
}

// Pointer returns a pointer to the record
func (r Record) Pointer() Pointer {
	return map[string]interface{}{
		"_collection": r.GetCollection(),
		"_id":         r.GetID(),
	}
}

// Clone allocates a new record with identical values
func (r Record) Clone() Record {
	cloned := Record(map[string]interface{}{})
	for k, v := range r {
		cloned[k] = v
	}
	return cloned
}

// Select returns the record with only the selected fields populated
func (r Record) Select(fields []string) Record {
	if len(fields) == 0 {
		return r
	}
	cloned := Record{}
	for _, field := range fields {
		cloned[field] = r[field]
	}
	return cloned
}

// Validate returns an error if the records collection, id, or fields are empty
func (r Record) Validate() error {
	if r.GetCollection() == "" {
		return errors.New("record validation: empty _collection")
	}
	if r.GetID() == "" {
		return errors.New("record validation: empty _id")
	}
	return nil
}

// Flatten flattens the record - all records are flattened before persistance
func (r Record) Flatten() (Record, error) {
	flattened, err := flat.Flatten(r, nil)
	if err != nil {
		return nil, err
	}
	return flattened, nil
}

// Unflatten unflattens the record
func (r Record) Unflatten() (Record, error) {
	unflattened, err := flat.Unflatten(r, nil)
	if err != nil {
		return nil, err
	}
	return unflattened, nil
}

// Encode marshals the record to flattened json
func (r Record) Encode() ([]byte, error) {
	data, err := r.Flatten()
	if err != nil {
		return nil, err
	}
	return json.Marshal(data)
}

// Scan scans the record into the provided struct - it uses json encoding
func (r Record) Scan(any any) error {
	uf, err := r.Unflatten()
	if err != nil {
		return err
	}
	bits, err := json.Marshal(&uf)
	if err != nil {
		return err
	}
	return json.Unmarshal(bits, &any)
}

func (r Record) key() []byte {
	return []byte(fmt.Sprintf("%s.%s", r.GetCollection(), r.GetID()))
}

func fieldIndexPrefix(collection string, fields []any) []byte {
	fieldValue := cast.ToString(fields[0])
	for _, field := range fields[1:] {
		fieldValue += fmt.Sprintf(".%s", cast.ToString(field))
	}
	return []byte(fmt.Sprintf("index.%s.%s.", collection, fieldValue))
}

func (r Record) fieldIndexKey(fields []string) []byte {
	var vals []any
	for _, f := range fields {
		vals = append(vals, r[f])
	}
	return []byte(fmt.Sprintf("%s.%s", string(fieldIndexPrefix(r.GetCollection(), vals)), r.GetID()))
}

// GetCollection gets the collection from the record
func (r Record) GetCollection() string {
	return cast.ToString(r["_collection"])
}

// GetID gets the id from the record
func (r Record) GetID() string {
	return cast.ToString(r["_id"])
}

// Get gets a field on the record
func (r Record) Get(field string) (any, bool) {
	val, ok := r[field]
	return val, ok
}

// Set sets a field on the record
func (r Record) Set(field string, val any) {
	r[field] = val
}

// Del deletes a field from the record
func (r Record) Del(field string) {
	delete(r, field)
}

// GetMetadata gets the metadata from the record
func (r Record) GetMetadata() MetaData {
	if _, ok := r["_metadata"].(MetaData); !ok {
		r["_metadata"] = MetaData{}
	}
	return r["_metadata"].(MetaData)
}

// GetExpiresAt gets the expiration timestamp of the record
func (r Record) GetExpiresAt() time.Time {
	return cast.ToTime(r["_expires_at"])
}

// SetCollection sets the collection on the record
func (r Record) SetCollection(collection string) {
	r["_collection"] = collection
}

// SetID sets the id on the record
func (r Record) SetID(id string) {
	r["_id"] = id
}

// SetMetadata sets the metadta on the record
func (r Record) SetMetadata(m MetaData) {
	r["_metadata"] = m
}

// SetExpiresAt sets the expiration timestamp on the record
func (r Record) SetExpiresAt(expires time.Time) {
	r["_expires_at"] = expires
}

// Where executes the where clauses against the record and returns true if it passes the clauses
func (r Record) Where(wheres []Where) bool {
	for _, w := range wheres {
		switch w.Op {
		case "==", "eq":
			if cast.ToString(r[w.Field]) != cast.ToString(w.Value) {
				return false
			}
		case "!=", "neq":
			if r[w.Field] == w.Value {
				return false
			}
		case ">", "gt":
			if cast.ToFloat64(r[w.Field]) > cast.ToFloat64(w.Value) {
				return false
			}
		case ">=", "gte":
			if cast.ToFloat64(r[w.Field]) >= cast.ToFloat64(w.Value) {
				return false
			}
		case "<", "lt":
			if cast.ToFloat64(r[w.Field]) < cast.ToFloat64(w.Value) {
				return false
			}
		case "<=", "lte":
			if cast.ToFloat64(r[w.Field]) <= cast.ToFloat64(w.Value) {
				return false
			}
		}
	}
	return true
}
