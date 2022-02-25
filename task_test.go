// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"testing"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
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

// DataGen populate data
func DataGen(scoll *mongo.Collection, total int) (*mongo.InsertManyResult, error) {
	ctx := context.Background()
	scoll.Drop(ctx)
	var docs []interface{}
	var ids []interface{}
	for i := 0; i < total; i++ {
		n := i + 1001
		auuid, err := uuid.New()
		if err != nil {
			return nil, err
		}
		ids = append(ids, n)
		num := n*n + Port
		doc := bson.D{{"_id", n},
			{"string", fmt.Sprintf("%06d-%v-%v-%v", i+1, num, n, num)},
			{"binary", auuid},
			{"bin1", primitive.Binary{Subtype: byte(1), Data: []byte(auuid[:])}},
			{"bin2", primitive.Binary{Subtype: byte(2), Data: []byte(auuid[:])}},
			{"bin3", primitive.Binary{Subtype: byte(3), Data: []byte(auuid[:])}},
			{"uuid", primitive.Binary{Subtype: byte(4), Data: []byte(auuid[:])}},
			{"int64", int64(num)},
			{"float64", float64(num)},
		}
		var list []int
		for n := 101; n < 110; n++ {
			list = append(list, n*n-n)
		}
		doc = append(doc, bson.D{{"array", list}}...)
		doc = append(doc, bson.D{{"subdoc", bson.D{{"level1", doc}}}}...)
		docs = append(docs, doc)
	}
	return scoll.InsertMany(ctx, docs)
}
