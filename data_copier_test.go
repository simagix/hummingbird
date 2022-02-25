// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"testing"

	"github.com/simagix/keyhole/mdb"
)

func TestDataCopier(t *testing.T) {
	ctx := context.Background()
	dbName, _ := mdb.SplitNamespace(TestNS)

	source, err := GetMongoClient(TestSourceURI)
	assertEqual(t, nil, err)
	err = source.Database(dbName).Drop(ctx)
	assertEqual(t, nil, err)

	target, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)
	err = target.Database(dbName).Drop(ctx)
	assertEqual(t, nil, err)

	filename := "testdata/data-only.json"
	inst, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	ws := inst.Workspace()
	err = ws.Reset()
	assertEqual(t, nil, err)
	err = DataCopier()
	assertEqual(t, nil, err)
}

func TestGetQualifiedCollections(t *testing.T) {
	includes, err := getQualifiedCollections(TestReplicaURI)
	assertEqual(t, nil, err)
	assertNotEqual(t, 0, len(includes))
}
