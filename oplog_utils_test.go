// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"testing"

	"github.com/simagix/keyhole/mdb"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestSkipOplog(t *testing.T) {
	filename := "testdata/config.json"
	inst, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	includes := []*Include{&Include{Namespace: "keyhole.*"}}
	inst.ResetIncludesTo(includes)
	reader, err := NewBSONReader(TestOplogFile)
	assertEqual(t, nil, err)
	var data []byte
	total := 0
	for {
		if data = reader.Next(); data == nil || total > 100 {
			break
		}
		var oplog Oplog
		bson.Unmarshal(data, &oplog)
		dbName, _ := mdb.SplitNamespace(oplog.Namespace)
		if dbName != "keyhole" {
			continue
		}
		total++
		assertEqual(t, false, SkipOplog(oplog))
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
