// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"testing"
	"time"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestConfigCopier(t *testing.T) {
	ctx := context.Background()
	dbName, collName := mdb.SplitNamespace(TestNS)

	source, err := GetMongoClient(TestSourceURI)
	assertEqual(t, nil, err)
	err = source.Database(dbName).Drop(ctx)
	assertEqual(t, nil, err)

	target, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)
	err = target.Database(dbName).Drop(ctx)
	assertEqual(t, nil, err)

	var doc bson.M
	err = source.Database("admin").RunCommand(ctx, bson.D{{"enableSharding", dbName}}).Decode(&doc)
	assertEqual(t, nil, err)
	locale := "zh_Hant"
	collation := options.Collation{Locale: locale}
	collOpts := options.CreateCollection()
	collOpts.SetCollation(&collation)
	err = source.Database(dbName).CreateCollection(ctx, collName, collOpts)
	assertEqual(t, nil, err)
	time.Sleep(10 * time.Second) // wait for config db to be updated

	filename := "testdata/config.json"
	inst, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	inst.DropCollections()
	err = ConfigCopier()
	assertEqual(t, nil, err)

	inst.ResetIncludesTo(nil)
	err = ConfigCopier()
	assertNotEqual(t, nil, err)

	inst.DropCollections()
	err = ConfigCopier()
	assertEqual(t, nil, err)
}
