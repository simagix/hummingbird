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

// ConfigDB contains config.databases
type ConfigDB struct {
	ID          string `bson:"_id"`
	Partitioned bool   `bson:"partitioned"`
	Primary     string `bson:"primary"`
}

// ConfigCollection contains config.collections
type ConfigCollection struct {
	ID               string `bson:"_id"`
	DefaultCollation bson.D `bson:"defaultCollation"`
	Dropped          bool   `bson:"dropped"`
	Key              bson.D `bson:"key"`
	Unique           bool   `bson:"unique"`
}

// ConfigCopier copies configuration including indexes from source to target
func ConfigCopier() error {
	now := time.Now()
	logger := gox.GetLogger("ConfigCopier")
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
	primaries := bson.M{}
	if len(targetShards) >= len(sourceShards) {
		for i := 0; i < len(sourceShards); i++ {
			primaries[sourceShards[i].ID] = targetShards[i].ID
		}
	} else {
		for i := 0; i < len(targetShards); i++ {
			primaries[sourceShards[i].ID] = targetShards[i].ID
		}
		idx := len(targetShards) - 1
		for i, j := idx, 0; i < len(sourceShards); i, j = i+1, j+1 {
			primaries[sourceShards[i].ID] = targetShards[j%len(targetShards)].ID
		}
	}
	if addShardingConfigs(sourceClient, targetClient, primaries); err != nil {
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
		for _, dbName := range dbNames {
			if inst.included[dbName] != nil {
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
				cmd := bson.D{{"addShardToZone", target.ID}, {"zone", tag}}
				var doc bson.M
				if err := client.Database("admin").RunCommand(context.Background(), cmd).Decode(&doc); err != nil {
					return fmt.Errorf(`addShardToZone to shard %v failed: %v`, target.ID, err)
				}
			}
		}
	}
	return nil
}

func addShardingConfigs(sourceClient *mongo.Client, targetClient *mongo.Client, primaries bson.M) error {
	now := time.Now()
	inst := GetMigratorInstance()
	logger := gox.GetLogger("addShardingConfigs")
	ctx := context.Background()
	query := bson.D{{"dropped", bson.D{{"$ne", true}}}}
	if len(inst.included) > 0 {
		dbNames := []string{}
		for ns := range inst.included {
			dbName, _ := mdb.SplitNamespace(ns)
			dbNames = append(dbNames, dbName)
		}
		query = bson.D{{"_id", bson.D{{"$in", dbNames}}}, {"dropped", bson.D{{"$ne", true}}}}
	}
	cursor, err := sourceClient.Database("config").Collection("databases").Find(ctx, query)
	if err != nil {
		return err
	}
	var doc bson.M
	for cursor.Next(ctx) {
		var cfg ConfigDB
		if err = cursor.Decode(&cfg); err != nil {
			return err
		}
		if cfg.ID == "admin" || cfg.ID == "local" || cfg.ID == "config" || cfg.ID == "test" {
			continue
		}
		newPrimary := primaries[cfg.Primary]
		if err = targetClient.Database("admin").RunCommand(ctx, bson.D{{"movePrimary", cfg.ID}, {"to", newPrimary}}).Decode(&doc); err != nil {
			return fmt.Errorf(`movePrimary for database %v to shard %v failed`, cfg.ID, primaries[cfg.Primary])
		}
		for {
			var doc bson.M
			coll := targetClient.Database("config").Collection("databases")
			if err = coll.FindOne(ctx, bson.D{{"_id", cfg.ID}}).Decode(&doc); err != nil {
				return fmt.Errorf("move primary shard failed: %v", err)
			}
			if doc["primary"] == newPrimary {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if cfg.Partitioned {
			logger.Infof(`enable sharding on database %v`, cfg.ID)
			if err = targetClient.Database("admin").RunCommand(ctx, bson.D{{"enableSharding", cfg.ID}}).Decode(&doc); err != nil {
				return fmt.Errorf(`enableSharding %v failed`, cfg.ID)
			}
		}
	}
	cursor.Close(ctx)
	if err = targetClient.Database("admin").RunCommand(ctx, bson.D{{"flushRouterConfig", 1}}).Decode(&doc); err != nil {
		return err
	}

	// shard collections config.collections
	if cursor, err = sourceClient.Database("config").Collection("collections").Find(ctx, bson.D{{"_id", bson.M{"$ne": "config.system.sessions"}}, {"dropped", false}}); err != nil {
		return fmt.Errorf(`find config.collections failed: %v`, err)
	}
	if cursor, err = sourceClient.Database("config").Collection("collections").Find(ctx, bson.D{{"_id", bson.M{"$ne": "config.system.sessions"}}}); err != nil {
		return fmt.Errorf(`find config.collections failed: %v`, err)
	}
	for cursor.Next(ctx) {
		var config ConfigCollection
		cursor.Decode(&config)
		if SkipNamespace(config.ID, inst.included) {
			continue
		}
		ns := config.ID
		include := inst.included[config.ID]
		if include != nil && include.To != "" {
			ns = include.To
			db, _ := mdb.SplitNamespace(ns)
			if err = targetClient.Database("admin").RunCommand(ctx, bson.D{{"enableSharding", db}}).Decode(&doc); err != nil {
				return fmt.Errorf(`enableSharding %v failed`, db)
			}
		}
		if err = targetClient.Database("admin").RunCommand(ctx,
			bson.D{{"shardCollection", ns}, {"key", config.Key}, {"unique", config.Unique},
				{"collation", config.DefaultCollation}}).Decode(&doc); err != nil {
			return fmt.Errorf(`shardCollection %v failed: %v`, ns, err)
		}
	}
	cursor.Close(ctx)
	logger.Infof("sharding config copied, took %v", time.Since(now))
	return nil
}
