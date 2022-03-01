// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"testing"

	"github.com/simagix/keyhole/mdb"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestSkipOplog(t *testing.T) {
	_, err := NewMigratorInstance("testdata/minimum.json")
	assertEqual(t, nil, err)
	oplogs, err := ReadCachedOplogs(TestOplogFile)
	assertEqual(t, nil, err)
	assertNotEqual(t, 0, len(oplogs))
	for _, oplog := range oplogs {
		assertEqual(t, false, SkipOplog(oplog))
		docs, ok := oplog.Object.Map()["applyOps"].(primitive.A)
		if ok {
			for _, doc := range docs {
				data, err := bson.Marshal(doc)
				assertEqual(t, nil, err)
				var alog Oplog
				bson.Unmarshal(data, &alog)
				assertEqual(t, false, SkipOplog(alog))
			}
			assertEqual(t, "c", oplog.Operation)
			assertEqual(t, "admin.$cmd", oplog.Namespace)
		}
	}
}

func TestBulkWrites(t *testing.T) {
	client, err := GetMongoClient(TestReplicaURI)
	assertEqual(t, nil, err)
	dbName, collName := mdb.SplitNamespace(TestNS)
	db := client.Database(dbName)
	ctx := context.Background()
	db.Drop(ctx)
	coll := client.Database(dbName).Collection(collName)
	opts := options.Update()
	opts.SetUpsert(true)
	update := bson.D{{"$set", bson.D{{"color", "Red"}}}}
	results, err := coll.UpdateOne(ctx, bson.D{{"_id", 123}}, update, opts)
	assertEqual(t, nil, err)
	assertEqual(t, int64(1), results.UpsertedCount)
	update = bson.D{{"$set", bson.D{{"color", "White"}}}}
	results, err = coll.UpdateOne(ctx, bson.D{{"_id", 123}}, update)
	assertEqual(t, int64(1), results.ModifiedCount)
	res, err := coll.DeleteOne(ctx, bson.D{{"_id", 123}})
	assertEqual(t, int64(1), res.DeletedCount)
}

func TestBulkWriteOplogs(t *testing.T) {
	_, err := NewMigratorInstance("testdata/config.json")
	reader, err := NewBSONReader("./testdata/shard02.220227.163423.028.bson.gz")
	assertEqual(t, nil, err)
	total := 0
	oplogs := []Oplog{}
	var data []byte
	for {
		if data = reader.Next(); data == nil {
			break
		}
		total++
		var oplog Oplog
		err = bson.Unmarshal(data, &oplog)
		assertEqual(t, nil, err)
		oplogs = append(oplogs, oplog)
	}
	assertEqual(t, 3806, total)
	assertEqual(t, 3806, len(oplogs))
	gox.GetLogger().SetLoggerLevel(gox.Debug)
	_, err = BulkWriteOplogs(oplogs)
	assertEqual(t, nil, err)
}
