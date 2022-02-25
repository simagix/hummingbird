// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CollectionCreator creates collections at target
func CollectionCreator() error {
	now := time.Now()
	logger := gox.GetLogger("CollectionCreator")
	logger.Remark("create collections")
	var err error
	ctx := context.Background()
	inst := GetMigratorInstance()
	sourceClient, err := GetMongoClient(inst.Source)
	targetClient, err := GetMongoClient(inst.Target)
	var dbNames []string
	if dbNames, err = GetQualifiedDBs(sourceClient, MetaDBName); err != nil {
		return err
	}
	for _, dbName := range dbNames {
		var cursor *mongo.Cursor
		if cursor, err = sourceClient.Database(dbName).ListCollections(ctx, bson.D{}); err != nil {
			return err
		}
		for cursor.Next(ctx) {
			var doc bson.M
			cursor.Decode(&doc)
			if doc["name"] == nil {
				continue
			}
			collName, ok := doc["name"].(string)
			if !ok || (doc["type"] != "" && doc["type"] != "collection") || strings.HasPrefix(collName, "system.") {
				continue
			}
			ns := fmt.Sprintf(`%v.%v`, dbName, collName)
			if inst.Included()[ns] != nil && inst.Included()[ns].To != "" {
				dbName, collName = mdb.SplitNamespace(inst.Included()[ns].To)
				ns = fmt.Sprintf(`%v.%v`, dbName, collName)
			} else if inst.SkipNamespace(ns) {
				continue
			}
			var collation *options.Collation
			var collOpts = options.CreateCollection()
			if doc["options"] != nil {
				m := doc["options"].(bson.M)
				if m["collation"] != nil {
					data, _ := bson.Marshal(m["collation"])
					bson.Unmarshal(data, &collation)
					collOpts.SetCollation(collation)
				}
				if m["capped"] != nil {
					collOpts.SetCapped(m["capped"].(bool))
				}
				if m["size"] != nil {
					collOpts.SetSizeInBytes(ToInt64(m["size"]))
				}
				if m["max"] != nil {
					collOpts.SetMaxDocuments(ToInt64(m["max"]))
				}
			}
			if err = targetClient.Database(dbName).CreateCollection(ctx, collName, collOpts); err != nil {
				logger.Errorf(`%v, error code: %v`, err, mdb.GetErrorCode(err))
				return err
			}
			time.Sleep(10 * time.Millisecond) // wait for metadata sync
		}
		cursor.Close(ctx)
	}
	logger.Infof("collections created, took %v", time.Since(now))
	return nil
}
