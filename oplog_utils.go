// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"time"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// GetTailableCursor returns a tailable cursor
func GetTailableCursor(client *mongo.Client, ts primitive.Timestamp) (*mongo.Cursor, error) {
	coll := client.Database("local").Collection("oplog.rs")
	opts := options.Find()
	opts.SetNoCursorTimeout(true)
	opts.SetCursorType(options.TailableAwait)
	opts.SetBatchSize(OplogBatchSize)
	opts.SetMaxAwaitTime(time.Second)
	// opts.SetOplogReplay(true)// doc says shouldn't be used
	filter := bson.M{"ts": bson.M{"$gte": ts}}
	return coll.Find(context.Background(), filter, opts)
}

// SkipOplog returns if oplog should be skipped
func SkipOplog(oplog Oplog) bool {
	if oplog.Namespace == "" {
		return true
	}
	var err error
	inst := GetMigratorInstance()
	dbName, collName := mdb.SplitNamespace(oplog.Namespace)
	if dbName == "" || dbName == "local" || dbName == "config" {
		return true
	}
	if collName == "$cmd" {
		for _, v := range oplog.Object {
			if v.Key == "dropDatabase" {
				return inst.SkipNamespace(dbName + ".*")
			} else if v.Key == "create" || v.Key == "createIndexes" || v.Key == "drop" || v.Key == "renameCollection" {
				collName = v.Value.(string)
				return inst.SkipNamespace(dbName + "." + collName)
			} else if v.Key == "applyOps" {
				oplogs, ok := v.Value.(primitive.A)
				if !ok {
					continue
				}
				for _, inlog := range oplogs {
					var doc Oplog
					var data []byte
					if data, err = bson.Marshal(inlog); err != nil {
						continue
					}
					if err = bson.Unmarshal(data, &doc); err != nil {
						continue
					}
					return inst.SkipNamespace(doc.Namespace)
				}
				return false // unknown, keep it for further investigation
			}
		}
		return false // unknown, keep it for further investigation
	}
	return inst.SkipNamespace(oplog.Namespace)
}
