// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import "testing"

const (
	TestNS         = "testdb.neutrino"
	TestSourceURI  = "mongodb://user:password@localhost/?compressors=zstd&readPreference=secondaryPreferred"
	TestTargetURI  = "mongodb://user:password@localhost:37017/?compressors=zstd&readPreference=secondaryPreferred"
	TestReplicaURI = "mongodb://admin:secret@localhost:30309/?compressors=zstd&replicaSet=replset"
)

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatalf("%s != %s", a, b)
	}
}

func assertNotEqual(t *testing.T, a interface{}, b interface{}) {
	if a == b {
		t.Fatalf("%s == %s", a, b)
	}
}

func TestNeutrino(t *testing.T) {
	version := "simagix/neutrino v0.0.1"
	Neutrino(version)
}
