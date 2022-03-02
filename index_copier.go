// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"fmt"
	"time"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
)

// IndexCopier copies indexes from source to target
func IndexCopier() error {
	now := time.Now()
	logger := gox.GetLogger("IndexCopier")
	inst := GetMigratorInstance()
	ws := inst.Workspace()
	status := "copy indexes"
	logger.Remark(status)
	err := ws.Log(status)
	if err != nil {
		return fmt.Errorf("update status failed: %v", err)
	}
	sourceClient, err := GetMongoClient(inst.Source)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	targetClient, err := GetMongoClient(inst.Target)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	logger.Info("create indexes")
	index := mdb.NewIndexStats("")
	index.SetFastMode(true) // disable shard key check
	if _, err = index.GetIndexes(sourceClient); err != nil {
		return err
	}
	indexes := []mdb.IndexNS{}
	if len(inst.Includes) > 0 {
		for _, filter := range inst.Includes {
			to := filter.Namespace
			if filter.To != "" {
				to = filter.To
			}
			indexes = append(indexes, mdb.IndexNS{From: filter.Namespace, To: to})
		}
	} else {
		var namespaces []string
		if namespaces, err = GetQualifiedNamespaces(sourceClient, true, MetaDBName); err != nil {
			return err
		}
		for _, ns := range namespaces {
			indexes = append(indexes, mdb.IndexNS{From: ns, To: ns})
		}
	}
	if err = index.CopyIndexesWithDest(targetClient, indexes, inst.IsDrop); err != nil {
		return err
	}
	logger.Infof("indexes copied, took %v", time.Since(now))
	return nil
}
