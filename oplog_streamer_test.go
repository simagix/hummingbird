// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"testing"
	"time"
)

func TestOplogStreamers(t *testing.T) {
	filename := "testdata/config.json"
	_, err := NewMigratorInstance(filename)
	assertEqual(t, nil, err)
	err = OplogStreamers()
	assertEqual(t, nil, err)
}

func TestCacheOplogs(t *testing.T) {
	streamer := OplogStreamer{SetName: "replset", Staging: "./workspace",
		URI: TestReplicaURI, isCache: true}
	go func() {
		err := streamer.CacheOplogs()
		assertEqual(t, nil, err)
	}()

	time.Sleep(2 * time.Second)
	streamer.LiveStream()
}
