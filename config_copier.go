// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"context"
	"fmt"
	"time"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ConfigCopier copies configuration including indexes from source to target
func ConfigCopier() error {
	now := time.Now()
	logger := gox.GetLogger("")
	inst := GetMigratorInstance()
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
	if inst.sourceStats.Cluster != mdb.Sharded || inst.targetStats.Cluster != mdb.Sharded {
		logger.Remarkf("configurations copied, took %v, source is %v and target is %v",
			time.Since(now), inst.sourceStats.Cluster, inst.targetStats.Cluster)
		return nil
	}

	var sourceClient, targetClient *mongo.Client
	if sourceClient, err = GetMongoClient(inst.Source); err != nil {
		return err
	}
	if targetClient, err = GetMongoClient(inst.Target); err != nil {
		return err
	}
	var sourceShards, targetShards []mdb.Shard
	if sourceShards, err = mdb.GetShards(sourceClient); err != nil {
		return err
	}
	if targetShards, err = mdb.GetShards(targetClient); err != nil {
		return err
	}
	if addShardTags(targetClient, sourceShards, targetShards); err != nil {
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

func addShardTags(client *mongo.Client, sourceShards []mdb.Shard, targetShards []mdb.Shard) error {
	var isZoneSharding bool
	for _, shard := range sourceShards {
		if len(shard.Tags) > 0 {
			isZoneSharding = true
			break
		}
	}
	if isZoneSharding {
		if len(sourceShards) != len(targetShards) {
			return fmt.Errorf("cannot migrate from %v to %v shards when zone sharding is configured", len(sourceShards), len(targetShards))
		}
		for i := 0; i < len(sourceShards); i++ {
			source := sourceShards[i]
			target := targetShards[i]
			for _, tag := range source.Tags {
				cmd := bson.D{{Key: "addShardToZone", Value: target.ID}, {Key: "zone", Value: tag}}
				var doc bson.M
				if err := client.Database("admin").RunCommand(context.Background(), cmd).Decode(&doc); err != nil {
					return fmt.Errorf(`addShardToZone to shard %v failed: %v`, target.ID, err)
				}
			}
		}
	}
	return nil
}
