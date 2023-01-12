package myjson

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"

	"github.com/autom8ter/myjson/kv"
	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
)

type casbinAdapter struct {
	db             Database
	collectionName string
}

func newCasbinAdapter(db Database) *casbinAdapter {
	return &casbinAdapter{db: db, collectionName: "casbin_policy"}
}

// CasbinPolicy ...
type CasbinPolicy struct {
	ID    string `json:"_id"`
	PType string `json:"p_type"`
	V0    string `json:"v0"`
	V1    string `json:"v1"`
	V2    string `json:"v2"`
	V3    string `json:"v3"`
	V4    string `json:"v4"`
	V5    string `json:"v5"`
}

func (a *CasbinPolicy) getHash() (string, error) {
	data := map[string]any{
		"p_type": a.PType,
		"v0":     a.V0,
		"v1":     a.V1,
		"v2":     a.V2,
		"v3":     a.V3,
		"v4":     a.V4,
		"v5":     a.V5,
	}
	s := sha1.New()
	bits, _ := json.Marshal(data)
	_, err := s.Write(bits)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(s.Sum(nil)), nil
}

func loadPolicyLine(policy CasbinPolicy, model model.Model) {
	lineText := policy.PType
	if policy.V0 != "" {
		lineText += ", " + policy.V0
	}
	if policy.V1 != "" {
		lineText += ", " + policy.V1
	}
	if policy.V2 != "" {
		lineText += ", " + policy.V2
	}
	if policy.V3 != "" {
		lineText += ", " + policy.V3
	}
	if policy.V4 != "" {
		lineText += ", " + policy.V4
	}
	if policy.V5 != "" {
		lineText += ", " + policy.V5
	}
	persist.LoadPolicyLine(lineText, model)
}

func makePolicy(ptype string, policys []string) CasbinPolicy {
	policy := CasbinPolicy{}
	policy.PType = ptype
	if len(policys) > 0 {
		policy.V0 = policys[0]
	}
	if len(policys) > 1 {
		policy.V1 = policys[1]
	}
	if len(policys) > 2 {
		policy.V2 = policys[2]
	}
	if len(policys) > 3 {
		policy.V3 = policys[3]
	}
	if len(policys) > 4 {
		policy.V4 = policys[4]
	}
	if len(policys) > 5 {
		policy.V5 = policys[5]
	}
	id, _ := policy.getHash()
	policy.ID = id
	return policy
}

func (a *casbinAdapter) clear(ctx context.Context, tx Tx) error {
	schema := a.db.GetSchema(ctx, a.collectionName)
	_, err := tx.ForEach(ctx, a.collectionName, ForEachOpts{}, func(d *Document) (bool, error) {
		if err := tx.Delete(ctx, a.collectionName, schema.GetPrimaryKey(d)); err != nil {
			return false, err
		}
		return true, nil
	})
	return err
}

func (a *casbinAdapter) insertPolicyLine(line *CasbinPolicy, tx Tx) error {
	doc, err := NewDocumentFrom(line)
	if err != nil {
		return err
	}
	_, err = tx.Create(context.Background(), a.collectionName, doc)
	return err
}

func (a *casbinAdapter) deletePolicyLine(policy *CasbinPolicy) (err error) {
	if policy.ID == "" {
		policy.ID, err = policy.getHash()
		if err != nil {
			return err
		}
	}
	return a.db.Tx(context.Background(), kv.TxOpts{IsBatch: true}, func(ctx context.Context, tx Tx) error {
		return tx.Delete(ctx, a.collectionName, policy.ID)
	})
}

// LoadPolicy loads policy from database.
func (a *casbinAdapter) LoadPolicy(model model.Model) error {
	var policys []CasbinPolicy
	_, err := a.db.ForEach(context.Background(), a.collectionName, ForEachOpts{}, func(d *Document) (bool, error) {
		var r CasbinPolicy
		if err := d.Scan(&r); err != nil {
			return false, err
		}
		if r.ID == "" {
			id, err := r.getHash()
			if err != nil {
				return false, err
			}
			r.ID = id
		}
		policys = append(policys, r)
		return true, nil
	})
	if err != nil {
		return err
	}
	for _, r := range policys {
		loadPolicyLine(r, model)
	}
	return nil
}

// SavePolicy saves all policy policys to the storage.
func (a *casbinAdapter) SavePolicy(model model.Model) (err error) {
	if err := a.db.Tx(context.Background(), kv.TxOpts{IsBatch: false}, func(ctx context.Context, tx Tx) error {
		if err := a.clear(ctx, tx); err != nil {
			return err
		}
		for ptype, ast := range model["p"] {
			for _, policy := range ast.Policy {
				policy := makePolicy(ptype, policy)

				err = a.insertPolicyLine(&policy, tx)
				if err != nil {
					return err
				}
			}
		}
		for ptype, ast := range model["g"] {
			for _, policy := range ast.Policy {
				policy := makePolicy(ptype, policy)
				err = a.insertPolicyLine(&policy, tx)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return
}

// AddPolicy adds a policy policy to the storage.
func (a *casbinAdapter) AddPolicy(sec string, ptype string, policy []string) error {
	if err := a.db.Tx(context.Background(), kv.TxOpts{IsBatch: false}, func(ctx context.Context, tx Tx) error {
		line := makePolicy(ptype, policy)
		return a.insertPolicyLine(&line, tx)
	}); err != nil {
		return err
	}
	return nil
}

// RemovePolicy removes a policy from the storage.
func (a *casbinAdapter) RemovePolicy(sec string, ptype string, policy []string) (err error) {
	line := makePolicy(ptype, policy)
	err = a.deletePolicyLine(&line)
	if err != nil {
		return
	}
	return err
}

// RemoveFilteredPolicy removes policys that match the filter from the storage.
func (a *casbinAdapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) (err error) {
	policy := CasbinPolicy{}
	policy.PType = ptype
	if fieldIndex <= 0 && 0 < fieldIndex+len(fieldValues) {
		policy.V0 = fieldValues[0-fieldIndex]
	}
	if fieldIndex <= 1 && 1 < fieldIndex+len(fieldValues) {
		policy.V1 = fieldValues[1-fieldIndex]
	}
	if fieldIndex <= 2 && 2 < fieldIndex+len(fieldValues) {
		policy.V2 = fieldValues[2-fieldIndex]
	}
	if fieldIndex <= 3 && 3 < fieldIndex+len(fieldValues) {
		policy.V3 = fieldValues[3-fieldIndex]
	}
	if fieldIndex <= 4 && 4 < fieldIndex+len(fieldValues) {
		policy.V4 = fieldValues[4-fieldIndex]
	}
	if fieldIndex <= 5 && 5 < fieldIndex+len(fieldValues) {
		policy.V5 = fieldValues[5-fieldIndex]
	}
	err = a.deletePolicyLine(&policy)
	if err != nil {
		return
	}
	return
}
