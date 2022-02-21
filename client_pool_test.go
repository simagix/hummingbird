// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import "testing"

func TestGetMongoClient(t *testing.T) {
	_, err := GetMongoClient("invalid uri")
	assertNotEqual(t, nil, err)

	_, err = GetMongoClient("mongodb://none-existing-host")
	assertNotEqual(t, nil, err)

	_, err = GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)
}

func TestGetMongoClientWait(t *testing.T) {
	client, err := GetMongoClientWait("mongodb://none-existing-host", 1)
	assertNotEqual(t, nil, err)

	client, err = GetMongoClientWait(TestTargetURI)
	assertEqual(t, nil, err)
	assertNotEqual(t, nil, client)
}
