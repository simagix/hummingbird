// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import "testing"

const (
	TestNS        = "testdb.neutrino"
	TestSourceURI = "mongodb://user:password@localhost/?compressors=zstd&readPreference=secondaryPreferred"
	TestTargetURI = "mongodb://admin:secret@localhost:30309/?compressors=zstd&replicaSet=replset"
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

func TestRun(t *testing.T) {
	version := "humingbird v0.0.1"
	Run(version)
}
