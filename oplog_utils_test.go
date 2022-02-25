// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestSkipOplog(t *testing.T) {
	_, err := NewMigratorInstance("testdata/minimum.json")
	assertEqual(t, nil, err)
	oplogs, err := ReadCachedOplogs(TestOplogFile)
	assertEqual(t, nil, err)
	assertNotEqual(t, 0, len(oplogs))
	for _, oplog := range oplogs {
		assertEqual(t, false, SkipOplog(oplog))
		docs, ok := oplog.Object.Map()["applyOps"].(primitive.A)
		if ok {
			for _, doc := range docs {
				data, err := bson.Marshal(doc)
				assertEqual(t, nil, err)
				var alog Oplog
				bson.Unmarshal(data, &alog)
				assertEqual(t, false, SkipOplog(alog))
			}
			assertEqual(t, "c", oplog.Operation)
			assertEqual(t, "admin.$cmd", oplog.Namespace)
		}
	}
}
