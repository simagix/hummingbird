// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"testing"
	"time"

	"github.com/simagix/keyhole/mdb"
)

func TestOplogStreamers(t *testing.T) {
	filename := "testdata/config.json"
	_, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	err = OplogStreamers()
	assertEqual(t, nil, err)
}

func TestCacheOplogs(t *testing.T) {
	inst, err := NewMigratorInstance("testdata/minimum.json")
	ws := inst.Workspace()
	ws.CleanUpWorkspace()
	ctx := context.Background()
	replset := "replset"
	streamer := OplogStreamer{SetName: replset, Staging: "./workspace",
		URI: TestReplicaURI, isCache: true}
	go func() {
		err := streamer.CacheOplogs()
		assertEqual(t, nil, err)
	}()

	client, err := GetMongoClient(TestReplicaURI)
	assertEqual(t, nil, err)
	dbName, _ := mdb.SplitNamespace(TestNS)
	client.Database(dbName).Drop(ctx)

	tclient, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)
	tclient.Database(dbName).Drop(ctx)

	DataGenMulti(client.Database(dbName), 4096, 3)

	time.Sleep(1 * time.Second)
	streamer.LiveStream()
	DataGenMulti(client.Database(dbName), 1024, 3)
	time.Sleep(1 * time.Second)
}
