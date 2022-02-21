// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"context"
	"fmt"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/mongo"
)

// Start starts a migration
func Start(filename string) error {
	gox.GetLogger("").Remarkf("start a migration from %v", filename)
	var err error
	var isConfig, isData, isOplog bool
	m, err := NewMigratorInstance(filename)
	if err != nil {
		return err
	}
	if m.Command == CommandIndex {
		return IndexCopier()
	} else if m.Command == CommandAll {
		isConfig = true
		isData = true
		isOplog = true
	} else if m.Command == CommandConfig {
		isConfig = true
	} else if m.Command == CommandData || m.Command == CommandDataOnly {
		if m.IsDrop {
			return fmt.Errorf(`cannot use {"drop": true} with {"command": "%v"}`, m.Command)
		}
		isData = true
		if m.Command == CommandData {
			isOplog = true
		}
	}

	if isData { // if balancer is running, exits
		if m.sourceStats.Cluster == mdb.Sharded {
			if err = CheckIfBalancerDisabled(m.Source); err != nil {
				return fmt.Errorf("source cluster error: %v", err)
			}
		}
		if m.targetStats.Cluster == mdb.Sharded {
			if err = CheckIfBalancerDisabled(m.Source); err != nil {
				return fmt.Errorf("target cluster error: %v", err)
			}
		}
	}

	if m.IsDrop {
		if err = DropCollections(); err != nil {
			return err
		}
	}

	if isOplog {
		OplogCopier()
	}

	if isConfig {
		if err = ConfigCopier(); err != nil {
			return err
		}
	}

	if isData {
		GetMigratorInstance().workspace.Reset() // reset meta data and clean up staging
		replicas, err := GetAllMongoProcURI(GetMigratorInstance().Source)
		fmt.Println(replicas)
		if err = DataCopier(); err != nil {
			return err
		}
	}
	return nil
}

// DropCollections drops all qualified collections
func DropCollections() error {
	inst := GetMigratorInstance()
	logger := gox.GetLogger("")
	ctx := context.Background()
	client, err := GetMongoClient(inst.Target)
	if err != nil {
		return err
	}
	if len(inst.Includes) == 0 { // drop all
		logger.Info("drop all target databases")
		dbNames, err := GetQualifiedDBs(client, MetaDBName)
		if err != nil {
			return err
		}
		for _, dbName := range dbNames {
			client.Database(dbName).Drop(ctx)
		}
	} else {
		namespaces, err := GetQualifiedNamespaces(client, true, MetaDBName)
		included := map[string]bool{}
		for _, ns := range namespaces {
			included[ns] = true
			dbName, _ := mdb.SplitNamespace(ns)
			included[dbName+".*"] = true
		}
		for _, include := range inst.Includes {
			ns := include.Namespace
			if include.To != "" {
				ns = include.To
			}
			dbName, collName := mdb.SplitNamespace(ns)
			if collName == "" || collName == "*" {
				if included[dbName+".*"] {
					logger.Info("drop database " + dbName)
					if err = client.Database(dbName).Drop(ctx); err != nil {
						return err
					}
				}
			} else if included[ns] {
				logger.Infof("drop namespace %v.%v", dbName, collName)
				if err = client.Database(dbName).Collection(collName).Drop(ctx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// CheckIfBalancerDisabled returns balancer state
func CheckIfBalancerDisabled(uri string) error {
	var err error
	var client *mongo.Client
	var enabled bool
	if client, err = GetMongoClient(uri); err != nil {
		return err
	}
	if enabled, err = IsBalancerEnabled(client); err != nil {
		return err
	} else if enabled {
		return fmt.Errorf("balancer is enabled")
	}
	return nil
}
