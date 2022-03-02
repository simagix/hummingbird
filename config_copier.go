// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"time"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ConfigChunk contains config.chunks
type ConfigChunk struct {
	Namespace string `json:"ns" bson:"ns"`
	Max       bson.D `json:"max" bson:"max"`
	Min       bson.D `json:"min" bson:"min"`
	Shard     string `json:"shard" bson:"shard"`
}

// ConfigCollection contains config.collections
type ConfigCollection struct {
	ID               string `bson:"_id"`
	DefaultCollation bson.D `bson:"defaultCollation"`
	Dropped          bool   `bson:"dropped"`
	Key              bson.D `bson:"key"`
	Unique           bool   `bson:"unique"`
}

// ConfigDB contains config.databases
type ConfigDB struct {
	ID          string `bson:"_id"`
	Partitioned bool   `bson:"partitioned"`
	Primary     string `bson:"primary"`
}

// ConfigCopier copies configuration including indexes from source to target
func ConfigCopier() error {
	now := time.Now()
	logger := gox.GetLogger("ConfigCopier")
	inst := GetMigratorInstance()
	ws := inst.Workspace()
	status := "copy configurations"
	logger.Remark(status)
	err := ws.Log(status)
	if err != nil {
		return fmt.Errorf("update status failed: %v", err)
	}
	if err = DoesDataExist(); err != nil {
		return err
	}
	if err = CollectionCreator(); err != nil {
		return err
	}
	if err = IndexCopier(); err != nil {
		return err
	}
	if inst.SourceStats().Cluster != mdb.Sharded || inst.TargetStats().Cluster != mdb.Sharded {
		status = fmt.Sprintf("configurations copied, took %v, source is %v and target is %v",
			time.Since(now), inst.SourceStats().Cluster, inst.TargetStats().Cluster)
		logger.Remark(status)
		if err = ws.Log(status); err != nil {
			return fmt.Errorf("update status failed: %v", err)
		}
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
	if err = addShardTags(targetClient, sourceShards, targetShards); err != nil {
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
	if err = addShardingConfigs(sourceClient, targetClient, primaries); err != nil {
		return err
	}
	if err = addChunks(sourceClient, targetClient, targetShards); err != nil {
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
	if len(inst.Includes) == 0 && len(dbNames) > 0 {
		return fmt.Errorf(`existing data detected, restart with {"drop": true} option`)
	}
	for _, dbName := range dbNames {
		if inst.Included()[dbName] != nil {
			return fmt.Errorf(`existing data detected, restart with {"drop": true} option`)
		}
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
	if len(inst.Included()) > 0 {
		dbNames := []string{}
		for ns := range inst.Included() {
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
			logger.Debugf(`enable sharding on database %v`, cfg.ID)
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
	query = bson.D{{"_id", bson.M{"$ne": "config.system.sessions"}}, {"dropped", false}}
	if cursor, err = sourceClient.Database("config").Collection("collections").Find(ctx, query); err != nil {
		return fmt.Errorf(`find config.collections failed: %v`, err)
	}
	for cursor.Next(ctx) {
		var config ConfigCollection
		cursor.Decode(&config)
		if inst.SkipNamespace(config.ID) {
			continue
		}
		ns := config.ID
		include := inst.Included()[config.ID]
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

func addChunks(sourceClient *mongo.Client, targetClient *mongo.Client, targetShards []mdb.Shard) error {
	now := time.Now()
	inst := GetMigratorInstance()
	logger := gox.GetLogger("addShardingConfigs")
	ctx := context.Background()
	chunksNeeded := len(targetShards)
	opts := options.Find()
	opts.SetSort(bson.D{{"ns", 1}, {"min", 1}})
	query := bson.D{{"ns", bson.M{"$ne": "config.system.sessions"}}, {"dropped", bson.M{"$ne": true}}}
	cursor, err := sourceClient.Database("config").Collection("chunks").Find(ctx, query, opts)
	if err != nil {
		return fmt.Errorf(`find config.chunks failed: %v`, err)
	}
	chunks := map[string][]ConfigChunk{}
	logger.Info("split chunks")
	for cursor.Next(ctx) {
		var cfg ConfigChunk
		if err = cursor.Decode(&cfg); err != nil {
			return fmt.Errorf("decode error: %v", err)
		}
		if inst.SkipNamespace(cfg.Namespace) {
			continue
		}
		if chunks[cfg.Namespace] == nil {
			chunks[cfg.Namespace] = []ConfigChunk{}
		}
		chunks[cfg.Namespace] = append(chunks[cfg.Namespace], cfg)
	}
	cursor.Close(ctx)
	for ns, arr := range chunks {
		if inst.SkipNamespace(ns) {
			continue
		}
		qfilter := inst.Included()[ns]
		if qfilter != nil && qfilter.To != "" {
			ns = qfilter.To
		}
		segment := len(arr) / chunksNeeded
		if len(arr) < chunksNeeded {
			return fmt.Errorf(`%v does not have enough chunks info to automatically split chunks`, ns)
		}
		count := 1
		for i, chunk := range arr {
			var doc bson.M
			if count == chunksNeeded {
				break
			} else if i == 0 || (len(arr) > chunksNeeded && i%segment != 0) {
				continue
			} else if err = targetClient.Database("admin").RunCommand(ctx, bson.D{{"split", ns}, {"middle", chunk.Min}}).Decode(&doc); err != nil {
				return fmt.Errorf(`split failed: %v`, err)
			}
			count++
		}
	}
	query = bson.D{{"ns", bson.M{"$ne": "config.system.sessions"}}}
	if len(inst.Included()) > 0 {
		namespaces := []string{}
		for _, include := range inst.Included() {
			namespaces = append(namespaces, include.To)
		}
		query = bson.D{{"ns", bson.D{{"$in", namespaces}}}}
	}
	if cursor, err = targetClient.Database("config").Collection("chunks").Find(ctx, query, opts); err != nil {
		return fmt.Errorf(`find config.chunks failed: %v`, err)
	}
	chunksMap := map[string][]ConfigChunk{}
	for cursor.Next(ctx) {
		var cfg ConfigChunk
		if err = cursor.Decode(&cfg); err != nil {
			return err
		}
		ns := cfg.Namespace
		if inst.Included()[ns] != nil && inst.Included()[ns].To != "" {
			ns = inst.Included()[ns].To
		}
		if chunksMap[ns] == nil {
			chunksMap[ns] = []ConfigChunk{}
		}
		chunksMap[ns] = append(chunksMap[ns], cfg)
	}
	cursor.Close(ctx)
	if chunksNeeded > 1 {
		for ns, arr := range chunksMap {
			for i, chunk := range arr {
				var doc bson.M
				bounds := []bson.D{chunk.Min, chunk.Max}
				if err = targetClient.Database("admin").RunCommand(ctx,
					bson.D{{"moveChunk", ns}, {"bounds", bounds}, {"to", targetShards[i].ID}}).Decode(&doc); err != nil {
					return fmt.Errorf(`moveChunk %v failed %v`, ns, err)
				}
				if i >= chunksNeeded {
					break
				}
			}
		}
	}
	logger.Infof("chunks added, took %v", time.Since(now))
	return nil
}
