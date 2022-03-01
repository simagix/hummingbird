// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/simagix/keyhole/mdb"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	// TestOplogFile holds oplogs
	TestOplogFile = "testdata/oplog.rs.bson.gz"
	// TestOplogFile holds docs of all data types
	TestAllDataTypesFile = "testdata/all-data-types.bson.gz"
)

func TestBSONReader(t *testing.T) {
	reader, err := NewBSONReader("none-exists")
	assertNotEqual(t, nil, err)

	reader, err = NewBSONReader(TestOplogFile)
	assertEqual(t, nil, err)
	cnt := 0
	for reader.Next() != nil {
		cnt++
	}
	if cnt == 0 {
		t.Fatal("not able to read data from:", TestOplogFile)
	}

}

func TestBSONReaderFile(t *testing.T) {
	reader, err := NewBSONReader(TestAllDataTypesFile)
	assertEqual(t, nil, err)
	uuidMap := map[string]bool{}
	var data []byte
	var oplogs []Oplog
	for {
		if data = reader.Next(); data == nil {
			break
		}
		var oplog Oplog
		err = bson.Unmarshal(data, &oplog)
		oplogs = append(oplogs, oplog)
		assertEqual(t, nil, err)
		key := fmt.Sprintf("%x", oplog.Object.Map()["user_uuid"].(primitive.Binary).Data)
		assertEqual(t, false, uuidMap[key])
		uuidMap[key] = true
	}
}

func TestBSONReaderNext(t *testing.T) {
	var oplogs []Oplog
	ctx := context.Background()
	filename := "uuid.bson.gz"
	dbName, collName := mdb.SplitNamespace(TestNS)
	client, err := GetMongoClient(TestSourceURI)
	assertEqual(t, nil, err)
	coll := client.Database(dbName).Collection(collName)
	filter := bson.D{{"uuid", bson.D{{"$exists", true}}}}
	_, err = coll.DeleteMany(ctx, filter)
	assertEqual(t, nil, err)
	_, err = DataGen(coll, 3000)
	assertEqual(t, nil, err)

	cursor, err := coll.Find(ctx, filter)
	for cursor.Next(ctx) {
		var doc bson.D
		cursor.Decode(&doc)
		oplogs = append(oplogs, Oplog{Namespace: TestNS, Operation: "i", Object: doc})
	}
	cursor.Close(ctx)

	reader, err := NewBSONReader(filename)
	assertEqual(t, nil, err)
	uuidMap := map[string]bool{}
	var data []byte
	total := 0
	for {
		if data = reader.Next(); data == nil {
			break
		}
		total++
		var oplog Oplog
		err = bson.Unmarshal(data, &oplog)
		assertEqual(t, nil, err)
		key := fmt.Sprintf("%x", oplog.Object.Map()["uuid"].(primitive.Binary).Data)
		assertEqual(t, false, uuidMap[key])
		uuidMap[key] = true
	}
	os.Remove(filename)
}
