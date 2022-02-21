// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"fmt"
	"time"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
)

// ConfigCopier copies configuration including indexes from source to target
func ConfigCopier() error {
	now := time.Now()
	logger := gox.GetLogger("")
	logger.Remark("copy configurations")
	err := DoesDataExist()
	if err != nil {
		return err
	}
	if err = CollectionCreator(); err != nil {
		return err
	}
	if err = IndexCopier(); err != nil {
		return err
	}
	logger.Infof("configurations copied, took %v", time.Since(now))
	return nil
}

// DoesDataExist check if data already exists
func DoesDataExist() error {
	inst := GetMigratorInstance()
	client, err := GetMongoClient(inst.Target)
	dbNames, err := GetQualifiedDBs(client, MetaDBName)
	if err != nil {
		return err
	}
	var dataExists bool
	if len(inst.Includes) == 0 && len(dbNames) > 0 {
		dataExists = true
	} else {
		included := map[string]bool{}
		for _, include := range inst.Includes {
			dbName, _ := mdb.SplitNamespace(include.Namespace)
			if include.To != "" {
				dbName, _ = mdb.SplitNamespace(include.To)
			}
			included[dbName] = true
		}
		for _, dbName := range dbNames {
			if included[dbName] {
				dataExists = true
				break
			}
		}
	}

	if dataExists {
		return fmt.Errorf(`existing data detected, restart with {"drop": true} option`)
	}
	return nil
}
