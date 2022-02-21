// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"testing"
)

func TestGetQualifiedDBs(t *testing.T) {
	client, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)
	dbs, err := GetQualifiedDBs(client, MetaDBName)
	assertEqual(t, nil, err)
	assertNotEqual(t, 0, len(dbs))
}

func TestGetQualifiedNamespaces(t *testing.T) {
	client, err := GetMongoClient(TestTargetURI)
	assertEqual(t, nil, err)
	namespaces, err := GetQualifiedNamespaces(client, true, MetaDBName)
	assertEqual(t, nil, err)
	assertNotEqual(t, 0, len(namespaces))

	namespaces, err = GetQualifiedNamespaces(client, false, MetaDBName)
	assertEqual(t, nil, err)
	assertNotEqual(t, 0, len(namespaces))
}

func TestGetAllMongoProcURI(t *testing.T) {
	replicas, err := GetAllMongoProcURI(TestSourceURI)
	assertEqual(t, nil, err)
	assertEqual(t, 2, len(replicas))

	replicas, err = GetAllMongoProcURI(TestTargetURI)
	assertEqual(t, nil, err)
	assertEqual(t, 1, len(replicas))
}

func TestAddSetName(t *testing.T) {
	uri := "mongodb://user:password@localhost/"
	str := addSetName(uri, "replset")
	assertEqual(t, uri+"?replicaSet=replset", str)

	uri = "mongodb://user:password@localhost/?w=1"
	str = addSetName(uri, "replset")
	assertEqual(t, uri+"&replicaSet=replset", str)
}
