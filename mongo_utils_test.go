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
