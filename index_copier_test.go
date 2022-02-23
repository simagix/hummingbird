// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"testing"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestIndexCopier(t *testing.T) {
	ctx := context.Background()
	source, err := GetMongoClient(TestSourceURI)
	assertEqual(t, nil, err)
	dbName, collName := mdb.SplitNamespace(TestNS)
	src := source.Database(dbName).Collection(collName)
	err = src.Drop(ctx)
	assertEqual(t, nil, err)

	locale := "zh_Hant"
	opts := options.Index()
	collation := options.Collation{Locale: locale}
	collOpts := options.CreateCollection()
	collOpts.SetCollation(&collation)
	err = source.Database(dbName).CreateCollection(ctx, collName, collOpts)
	assertEqual(t, nil, err)
	opts.SetCollation(&collation)
	imodel := mongo.IndexModel{Keys: bson.D{{"_id", 1}}, Options: opts}
	_, err = src.Indexes().CreateOne(ctx, imodel)
	assertEqual(t, nil, err)

	target, err := GetMongoClient(TestReplicaURI)
	assertEqual(t, nil, err)
	tgt := target.Database(dbName).Collection(collName)
	err = tgt.Drop(ctx)
	assertEqual(t, nil, err)

	scursor, err := src.Indexes().List(ctx)
	assertEqual(t, nil, err)
	scursor.Next(ctx)
	var sindex mdb.Index
	bson.Unmarshal(scursor.Current, &sindex)
	defer scursor.Close(ctx)
	assertEqual(t, locale, sindex.Collation.Map()["locale"])

	filename := "testdata/index.json"
	_, err = NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	err = IndexCopier()
	assertEqual(t, nil, err)

	tcursor, err := tgt.Indexes().List(ctx)
	assertEqual(t, nil, err)
	tcursor.Next(ctx)
	var tindex mdb.Index
	bson.Unmarshal(tcursor.Current, &tindex)
	defer tcursor.Close(ctx)
	assertEqual(t, locale, tindex.Collation.Map()["locale"])

	filename = "testdata/minimum.json"
	_, err = NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	err = IndexCopier()
	assertEqual(t, nil, err)
}
