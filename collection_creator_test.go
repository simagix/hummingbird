// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"context"
	"testing"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestCollectionCreator(t *testing.T) {
	ctx := context.Background()
	dbName, collName := mdb.SplitNamespace(TestNS)

	source, err := GetMongoClient(TestSourceURI)
	assertEqual(t, nil, err)
	src := source.Database(dbName).Collection(collName)
	err = src.Drop(ctx)
	assertEqual(t, nil, err)

	target, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)
	tgt := target.Database(dbName).Collection(collName)
	err = tgt.Drop(ctx)
	assertEqual(t, nil, err)

	locale := "zh_Hant"
	collation := options.Collation{Locale: locale}
	collOpts := options.CreateCollection()
	collOpts.SetCollation(&collation)
	err = source.Database(dbName).CreateCollection(ctx, collName, collOpts)
	assertEqual(t, nil, err)

	filename := "testdata/config.json"
	m, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	DropCollections()
	err = CollectionCreator()
	assertEqual(t, nil, err)

	m.Includes = nil
	DropCollections()
	err = CollectionCreator()
	assertEqual(t, nil, err)
}
