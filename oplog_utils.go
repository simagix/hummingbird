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

// OplogWriteModel stores namespace and writeModel
type OplogWriteModel struct {
	Namespace  string
	Operation  string
	WriteModel mongo.WriteModel
}

// GetTailableCursor returns a tailable cursor
func GetTailableCursor(client *mongo.Client, ts *primitive.Timestamp) (*mongo.Cursor, error) {
	coll := client.Database("local").Collection("oplog.rs")
	opts := options.Find()
	opts.SetNoCursorTimeout(true)
	opts.SetCursorType(options.TailableAwait)
	opts.SetBatchSize(OplogBatchSize)
	opts.SetMaxAwaitTime(time.Second)
	filter := bson.M{"ts": bson.M{"$gte": ts}}
	return coll.Find(context.Background(), filter, opts)
}

// SkipOplog returns if oplog should be skipped
func SkipOplog(oplog Oplog) bool {
	dbName, collName := mdb.SplitNamespace(oplog.Namespace)
	if dbName == "" || dbName == "local" || dbName == "config" {
		return true
	}
	var err error
	inst := GetMigratorInstance()
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

// BulkWriteOplogsResult stores results
type BulkWriteOplogsResult struct {
	DeletedCount  int64
	InsertedCount int64
	ModifiedCount int64
	UpsertedCount int64
	TotalCount    int64
}

// BulkWriteOplogs applies oplogs in bulk
func BulkWriteOplogs(oplogs []Oplog) (*BulkWriteOplogsResult, error) {
	var results = BulkWriteOplogsResult{}
	inst := GetMigratorInstance()
	logger := gox.GetLogger("BulkWriteOplogs")
	client, err := GetMongoClient(inst.Target)
	if err != nil {
		return &results, fmt.Errorf("GetMongoClient failed: %v", err)
	}
	opts := options.BulkWrite()
	modelsMap := map[string][]OplogWriteModel{}
	for _, oplog := range oplogs {
		if SkipOplog(oplog) {
			continue
		}
		writeModels := GetWriteModels(oplog)
		for _, wmodel := range writeModels {
			modelsMap[wmodel.Namespace] = append(modelsMap[wmodel.Namespace], wmodel)
		}
	}
	ctx := context.Background()
	var result *mongo.BulkWriteResult
	missed := 0
	for ns, wmodels := range modelsMap {
		inserts := []mongo.WriteModel{}
		others := []mongo.WriteModel{}
		dbName, collName := mdb.SplitNamespace(ns)
		coll := client.Database(dbName).Collection(collName)
		for _, wmodel := range wmodels {
			if wmodel.Operation == "i" {
				if len(others) > 0 { // flush the others
					opts.SetOrdered(true)
					result, err = coll.BulkWrite(ctx, others, opts)
					if result != nil {
						results.DeletedCount += result.DeletedCount
						results.ModifiedCount += result.ModifiedCount
						results.UpsertedCount += result.UpsertedCount
					}
					updated := result.DeletedCount + result.ModifiedCount + result.UpsertedCount
					if int(updated) != len(others) {
						extra := 0
						for _, other := range others[updated:] {
							if result, err = coll.BulkWrite(ctx, []mongo.WriteModel{other}); err != nil {
								logger.Warnf("BulkWrites exception: %v", err)
							} else {
								results.DeletedCount += result.DeletedCount
								results.UpsertedCount += result.UpsertedCount
								results.ModifiedCount += result.ModifiedCount
								extra += int(result.DeletedCount + result.UpsertedCount + result.ModifiedCount)
							}
						}
						missed += (len(others) - int(updated) - int(extra))
					}
					others = []mongo.WriteModel{}
				}
				inserts = append(inserts, wmodel.WriteModel)
			} else {
				if len(inserts) > 0 { // flush inserts
					opts.SetOrdered(false)
					if result, err = coll.BulkWrite(ctx, inserts, opts); err != nil {
						if mdb.IsDuplicateKeyError(err) {
							results.InsertedCount += int64(len(inserts))
						} else {
							logger.Warnf("BulkWrites exception: %v", err)
						}
					} else {
						results.InsertedCount += result.InsertedCount
					}
					inserts = []mongo.WriteModel{}
				}
				others = append(others, wmodel.WriteModel)
			}
		}
		if len(inserts) > 0 { // flush inserts
			opts.SetOrdered(false)
			if result, err = coll.BulkWrite(ctx, inserts, opts); err != nil {
				if mdb.IsDuplicateKeyError(err) {
					results.InsertedCount += int64(len(inserts))
				} else {
					logger.Warnf("BulkWrites exception: %v", err)
				}
			} else {
				results.InsertedCount += result.InsertedCount
			}
		}
		if len(others) > 0 { // flush the others
			opts.SetOrdered(true)
			result, err = coll.BulkWrite(ctx, others, opts)
			if result != nil {
				results.DeletedCount += result.DeletedCount
				results.ModifiedCount += result.ModifiedCount
				results.UpsertedCount += result.UpsertedCount
			}
			updated := result.DeletedCount + result.ModifiedCount + result.UpsertedCount
			if int(updated) != len(others) {
				extra := 0
				for _, other := range others[updated:] {
					if result, err = coll.BulkWrite(ctx, []mongo.WriteModel{other}); err != nil {
						logger.Warnf("BulkWrites exception: %v", err)
					} else {
						results.DeletedCount += result.DeletedCount
						results.UpsertedCount += result.UpsertedCount
						results.ModifiedCount += result.ModifiedCount
						extra += int(result.DeletedCount + result.UpsertedCount + result.ModifiedCount)
					}
				}
				missed += (len(others) - int(updated) - int(extra))
			}
			others = []mongo.WriteModel{}
		}
	}
	results.TotalCount = results.InsertedCount + results.ModifiedCount + results.DeletedCount + results.UpsertedCount
	if int(results.TotalCount) < len(oplogs) {
		logger.Debugf("oplogs:%v, missed:%v, inserted:%v, modified:%v, deleted:%v, upserted:%v",
			len(oplogs), missed,
			results.InsertedCount, results.ModifiedCount, results.DeletedCount, results.UpsertedCount)
	}
	return &results, nil
}

// GetWriteModels returns WriteModel from an oplog
func GetWriteModels(oplog Oplog) []OplogWriteModel {
	inst := GetMigratorInstance()
	ns := inst.GetToNamespace(oplog.Namespace)
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
			gox.GetLogger().Debugf("c found applyOps %d", len(wmodels))
		}
		return wmodels
	case "d":
		op := mongo.NewDeleteOneModel()
		op.SetFilter(oplog.Object)
		return []OplogWriteModel{OplogWriteModel{ns, oplog.Operation, op}}
	case "i":
		op := mongo.NewInsertOneModel()
		op.SetDocument(oplog.Object)
		return []OplogWriteModel{OplogWriteModel{ns, oplog.Operation, op}}
	case "n":
		return nil
	case "u":
		var isUpdate bool
		o := oplog.Object
		for _, v := range oplog.Object {
			if v.Key != "$v" && strings.HasPrefix(v.Key, "$") {
				isUpdate = true
				o = bson.D{{v.Key, v.Value}}
				break
			}
		}
		if isUpdate {
			op := mongo.NewUpdateOneModel()
			op.SetFilter(oplog.Query)
			op.SetUpdate(o)
			return []OplogWriteModel{OplogWriteModel{ns, oplog.Operation, op}}
		}
		op := mongo.NewReplaceOneModel()
		op.SetFilter(oplog.Query)
		op.SetReplacement(o)
		return []OplogWriteModel{OplogWriteModel{ns, oplog.Operation, op}}
	}
	return nil
}
