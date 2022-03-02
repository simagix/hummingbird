// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"fmt"

	"github.com/simagix/keyhole"
	"go.mongodb.org/mongo-driver/bson"
)

// Compare compares migration results
func Compare(filename string) error {
	inst, err := NewMigratorInstance(filename)
	if err != nil {
		return fmt.Errorf("NewMigratorInstance failed: %v", err)
	}
	comparator, err := keyhole.NewComparator(inst.Source, inst.Target)
	if err != nil {
		return fmt.Errorf("NewComparator failed: %v", err)
	}
	filters := []keyhole.Filter{}
	for _, inc := range inst.Included() {
		query := bson.D{}
		if len(inc.Filter) > 0 {
			query = inc.Filter
		}
		filters = append(filters, keyhole.Filter{NS: inc.Namespace, Query: query, TargetNS: inc.To})
	}
	return comparator.Compare(filters, Port)
}
