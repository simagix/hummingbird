// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"testing"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
)

func TestCopyData(t *testing.T) {
	ctx := context.Background()
	dbName, collName := mdb.SplitNamespace(TestNS)
	source, err := GetMongoClient(TestSourceURI)
	assertEqual(t, nil, err)
	src := source.Database(dbName).Collection(collName)
	src.Drop(ctx)
	docs := []interface{}{}
	for i := 100; i < 110; i++ {
		docs = append(docs, bson.D{{"_id", i}})
	}
	_, err = src.InsertMany(ctx, docs)
	assertEqual(t, nil, err)

	target, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)
	tgt := target.Database(dbName).Collection(collName)
	tgt.Drop(ctx)
	docs = []interface{}{}
	for i := 100; i < 103; i++ {
		docs = append(docs, bson.D{{"_id", i}})
	}
	_, err = tgt.InsertMany(ctx, docs)
	assertEqual(t, nil, err)

	task := &Task{IDs: []interface{}{100, 109}, SourceCounts: 10}
	err = task.CopyData(src, tgt)
	assertEqual(t, nil, err)
	count, err := tgt.CountDocuments(ctx, bson.D{})
	assertEqual(t, nil, err)
	assertEqual(t, 10, int(count))

	tgt.Drop(ctx)
	err = task.CopyData(src, tgt)
	assertEqual(t, nil, err)
	count, err = tgt.CountDocuments(ctx, bson.D{})
	assertEqual(t, nil, err)
	assertEqual(t, 10, int(count))
}
