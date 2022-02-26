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

// BulkWriteOplogs applies oplogs in bulk
func BulkWriteOplogs(oplogs []Oplog) error {
	var inserted int64
	var modified int64
	var deleted int64
	inst := GetMigratorInstance()
	logger := gox.GetLogger("BulkWriteOplogs")
	client, err := GetMongoClient(inst.Target)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	opts := options.BulkWrite()
	models := map[string][]mongo.WriteModel{}
	for _, oplog := range oplogs {
		writeModels := GetWriteModels(oplog)
		for _, wmodel := range writeModels {
			models[wmodel.Namespace] = append(models[wmodel.Namespace], wmodel.WriteModel)
		}
	}
	ctx := context.Background()
	var result *mongo.BulkWriteResult
	for ns, model := range models {
		inserts := []mongo.WriteModel{}
		others := []mongo.WriteModel{}
		dbName, collName := mdb.SplitNamespace(ns)
		coll := client.Database(dbName).Collection(collName)
		for _, write := range model {
			buf, _ := bson.Marshal(write)
			var doc bson.M
			bson.Unmarshal(buf, &doc)
			if doc["document"] != nil {
				if len(others) > 0 { // flush the others
					opts.SetOrdered(true)
					if result, err = coll.BulkWrite(ctx, others, opts); err != nil {
						logger.Warnf("BulkWrites exception: %v", err)
					} else {
						deleted += result.DeletedCount
						modified += result.ModifiedCount
					}
					others = []mongo.WriteModel{}
				}
				inserts = append(inserts, write)
			} else {
				if len(inserts) > 0 { // flush inserts
					opts.SetOrdered(false)
					if result, err = coll.BulkWrite(ctx, inserts, opts); err != nil && !mdb.IsDuplicateKeyError(err) {
						logger.Warnf("BulkWrites exception: %v", err)
					} else {
						inserted += result.InsertedCount
					}
					inserts = []mongo.WriteModel{}
				}
				others = append(others, write)
			}
		}
		if len(inserts) > 0 { // flush inserts
			opts.SetOrdered(false)
			if result, err = coll.BulkWrite(ctx, inserts, opts); err != nil && !mdb.IsDuplicateKeyError(err) {
				logger.Warnf("BulkWrites exception: %v", err)
			} else {
				inserted += result.InsertedCount
			}
		}
		if len(others) > 0 { // flush the others
			opts.SetOrdered(true)
			if result, err = coll.BulkWrite(ctx, others, opts); err != nil {
				logger.Warnf("BulkWrites exception: %v", err)
			} else {
				deleted += result.DeletedCount
				modified += result.ModifiedCount
			}
		}
	}
	logger.Infof("oplogs %v, inserted: %v, modified: %v, deleted: %v", len(oplogs), inserted, modified, deleted)
	return nil
}

// OplogWriteModel stores namespace and write model
type OplogWriteModel struct {
	Namespace  string
	WriteModel mongo.WriteModel
}

// GetWriteModels returns WriteModel from an oplog
func GetWriteModels(oplog Oplog) []OplogWriteModel {
	inst := GetMigratorInstance()
	ns := inst.GetToNamespace(oplog.Namespace)
	fmt.Println(oplog.Namespace, ns)
	switch oplog.Operation {
	case "c":
		var err error
		wmodels := []OplogWriteModel{}
		for _, v := range oplog.Object {
			if v.Key != "applyOps" { // not supported yet
				continue
			}
			oplogs, ok := v.Value.(primitive.A)
			if !ok {
				break
			}
			for _, log := range oplogs {
				var oplog Oplog
				var data []byte
				if data, err = bson.Marshal(log); err != nil {
					break
				}
				if err = bson.Unmarshal(data, &oplog); err != nil {
					break
				}
				wmodels = append(wmodels, GetWriteModels(oplog)...)
			}
		}
		fmt.Println("c found applyOps", len(wmodels))
		return wmodels
	case "d":
		op := mongo.NewDeleteOneModel()
		op.SetFilter(oplog.Object)
		return []OplogWriteModel{OplogWriteModel{ns, op}}
	case "i":
		op := mongo.NewInsertOneModel()
		op.SetDocument(oplog.Object)
		return []OplogWriteModel{OplogWriteModel{ns, op}}
	case "n":
		fmt.Println("n ignored")
		return nil
	case "u":
		var isUpdate bool
		o := oplog.Object
		for _, v := range oplog.Object {
			if v.Key != "$v" && strings.HasPrefix(v.Key, "$") {
				isUpdate = true
				o = bson.D{{Key: v.Key, Value: v.Value}}
				break
			}
		}
		if isUpdate {
			op := mongo.NewUpdateOneModel()
			op.SetFilter(oplog.Query)
			op.SetUpdate(o)
			return []OplogWriteModel{OplogWriteModel{ns, op}}
		}
		op := mongo.NewReplaceOneModel()
		op.SetFilter(oplog.Query)
		op.SetReplacement(o)
		return []OplogWriteModel{OplogWriteModel{ns, op}}
	}
	return nil
}
