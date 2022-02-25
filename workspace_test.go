// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestDropMetaDB(t *testing.T) {
	ws := &Workspace{}
	err := ws.DropMetaDB()
	assertNotEqual(t, nil, err)

	ws = &Workspace{dbName: MetaDBName, dbURI: TestReplicaURI}
	err = ws.DropMetaDB()
	assertEqual(t, nil, err)
}

func TestCleanUpWorkspace(t *testing.T) {
	ws := &Workspace{}
	err := ws.CleanUpWorkspace()
	assertNotEqual(t, nil, err)

	os.Mkdir(DefaultStaging, 0755)
	filename := DefaultStaging + "/replset.index"
	filenames := []string{DefaultStaging + "/file1.bson.gz", DefaultStaging + "/file2.bson.gz"}
	err = ioutil.WriteFile(filename, []byte(strings.Join(filenames, "\n")), 0644)
	assertEqual(t, true, DoesFileExist(filename))
	assertEqual(t, nil, err)
	ws = &Workspace{staging: DefaultStaging}
	err = ws.CleanUpWorkspace()
	assertNotEqual(t, nil, err)

	for _, f := range filenames {
		err = ioutil.WriteFile(f, []byte(f), 0644)
		assertEqual(t, nil, err)
		assertEqual(t, true, DoesFileExist(f))
	}
	err = ioutil.WriteFile(filename, []byte(strings.Join(filenames, "\n")), 0644)
	assertEqual(t, true, DoesFileExist(filename))
	assertEqual(t, nil, err)
	ws = &Workspace{staging: DefaultStaging}
	err = ws.CleanUpWorkspace()
	assertEqual(t, nil, err)

	assertEqual(t, false, DoesFileExist(filename))
	for _, f := range filenames {
		assertEqual(t, false, DoesFileExist(f))
		os.Remove(f)
	}
	os.Remove(filename)
}

func TestReset(t *testing.T) {
	ws := &Workspace{}
	err := ws.Reset()
	assertNotEqual(t, nil, err)

	ws = &Workspace{dbName: MetaDBName, dbURI: TestReplicaURI}
	err = ws.Reset()
	assertNotEqual(t, nil, err)

	ws = &Workspace{dbName: MetaDBName, dbURI: TestReplicaURI, staging: DefaultStaging}
	err = ws.Reset()
	assertEqual(t, nil, err)
}

func TestResetLongRunningTasks(t *testing.T) {
	ctx := context.Background()
	client, err := GetMongoClient(TestReplicaURI)
	assertEqual(t, nil, err)
	coll := client.Database(MetaDBName).Collection(Tasks)
	coll.Drop(ctx)
	filter := bson.D{{"_id", primitive.NewObjectID()}}
	update := bson.D{{"$set", bson.D{{"status", TaskProcessing}, {"begin_time", time.Now()}}}}
	opts := options.Update()
	opts.SetUpsert(true)
	_, err = coll.UpdateOne(ctx, filter, update, opts)
	assertEqual(t, nil, err)

	ws := &Workspace{dbName: MetaDBName, dbURI: TestReplicaURI}
	modified, err := ws.ResetLongRunningTasks(0 * time.Minute)
	assertNotEqual(t, nil, err)
	modified, err = ws.ResetLongRunningTasks(-10 * time.Minute)
	assertEqual(t, nil, err)
	assertEqual(t, 0, modified)

	update = bson.D{{"$set", bson.D{{"status", TaskProcessing}, {"begin_time", time.Now().Add(-30 * time.Minute)}}}}
	_, err = coll.UpdateOne(context.Background(), filter, update, opts)
	assertEqual(t, nil, err)
	modified, err = ws.ResetLongRunningTasks(-10 * time.Minute)
	assertEqual(t, nil, err)
	assertEqual(t, 1, modified)
}
