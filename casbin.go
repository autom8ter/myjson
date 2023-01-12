package myjson

import (
	"fmt"

	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
)

// casbinAdapter is the file adapter for Casbin.
// It can load policy from file or save policy to file.
type casbinAdapter struct {
	schema CollectionSchema
}

// NewcasbinAdapter is the constructor for casbinAdapter.
func NewcasbinAdapter(schema CollectionSchema) *casbinAdapter {
	return &casbinAdapter{schema: schema}
}

// LoadPolicy loads all policy rules from the storage.
func (a *casbinAdapter) LoadPolicy(model model.Model) error {
	for _, policy := range a.schema.Policies() {
		err := persist.LoadPolicyLine(policy, model)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *casbinAdapter) UpdatePolicy(sec string, ptype string, oldRule, newRule []string) error {
	return fmt.Errorf("not implemented")
}

func (a *casbinAdapter) UpdatePolicies(sec string, ptype string, oldRules, newRules [][]string) error {
	return fmt.Errorf("not implemented")
}

func (a *casbinAdapter) UpdateFilteredPolicies(sec string, ptype string, newRules [][]string, fieldIndex int, fieldValues ...string) ([][]string, error) {
	return nil, fmt.Errorf("not implemented")
}

// SavePolicy saves all policy rules to the storage.
func (a *casbinAdapter) SavePolicy(model model.Model) error {
	return nil
}

// AddPolicy adds a policy rule to the storage.
func (a *casbinAdapter) AddPolicy(sec string, ptype string, rule []string) error {
	return fmt.Errorf("not implemented")
}

// AddPolicies adds policy rules to the storage.
func (a *casbinAdapter) AddPolicies(sec string, ptype string, rules [][]string) error {
	return fmt.Errorf("not implemented")
}

// RemovePolicy removes a policy rule from the storage.
func (a *casbinAdapter) RemovePolicy(sec string, ptype string, rule []string) error {
	return fmt.Errorf("not implemented")
}

// RemovePolicies removes policy rules from the storage.
func (a *casbinAdapter) RemovePolicies(sec string, ptype string, rules [][]string) error {
	return fmt.Errorf("not implemented")
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
func (a *casbinAdapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return fmt.Errorf("not implemented")
}
