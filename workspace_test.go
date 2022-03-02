// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"io/ioutil"
	"os"
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
	filenames := []string{DefaultStaging + "/file1.bson.gz", DefaultStaging + "/file2.bson.gz"}
	ws = &Workspace{staging: DefaultStaging}

	for _, f := range filenames {
		err = ioutil.WriteFile(f, []byte(f), 0644)
		assertEqual(t, nil, err)
		assertEqual(t, true, DoesFileExist(f))
	}
	ws = &Workspace{staging: DefaultStaging}
	err = ws.CleanUpWorkspace()
	assertEqual(t, nil, err)

	for _, f := range filenames {
		assertEqual(t, false, DoesFileExist(f))
		os.Remove(f)
	}
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
	coll := client.Database(MetaDBName).Collection(MetaTasks)
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

func TestGetOplogTimestamp(t *testing.T) {
	ws := &Workspace{dbName: MetaDBName, dbURI: TestReplicaURI}
	replset := "replset"
	ts := primitive.Timestamp{T: uint32(time.Now().Unix())}
	err := ws.SaveOplogTimestamp(replset, ts)
	assertEqual(t, nil, err)
	tstamp := ws.GetOplogTimestamp(replset)
	assertEqual(t, ts.T, tstamp.T)
}

func TestResetParentTask(t *testing.T) {
	ctx := context.Background()
	ws := &Workspace{dbName: MetaDBName, dbURI: TestReplicaURI}
	replset := "replset"
	ws.Reset()
	parentID := primitive.NewObjectID()
	parentTask := Task{ID: parentID, SetName: replset, Status: TaskSplitting}
	tasks := []*Task{
		&parentTask,
		&Task{ID: primitive.NewObjectID(), SetName: replset, Status: TaskAdded, ParentID: &parentID},
	}
	err := ws.InsertTasks(tasks)
	assertEqual(t, nil, err)

	client, err := GetMongoClient(ws.dbURI)
	coll := client.Database(MetaDBName).Collection(MetaTasks)

	count, err := coll.CountDocuments(ctx, bson.M{"status": TaskSplitting})
	assertEqual(t, nil, err)
	assertEqual(t, int64(1), count)

	count, err = coll.CountDocuments(ctx, bson.M{"status": TaskAdded})
	assertEqual(t, nil, err)
	assertEqual(t, int64(1), count)

	err = ws.ResetParentTask(parentTask)
	assertEqual(t, nil, err)

	count, err = coll.CountDocuments(ctx, bson.M{"status": TaskSplitting})
	assertEqual(t, nil, err)
	assertEqual(t, int64(0), count)

	count, err = coll.CountDocuments(ctx, bson.M{"status": TaskAdded})
	assertEqual(t, nil, err)
	assertEqual(t, int64(1), count)
}

func TestResetProcessingTasks(t *testing.T) {
	ctx := context.Background()
	ws := &Workspace{dbName: MetaDBName, dbURI: TestReplicaURI}
	replset := "replset"
	ws.Reset()
	tasks := []*Task{&Task{SetName: replset, Status: TaskProcessing}}
	err := ws.InsertTasks(tasks)
	assertEqual(t, nil, err)
	client, err := GetMongoClient(ws.dbURI)
	coll := client.Database(MetaDBName).Collection(MetaTasks)

	count, err := coll.CountDocuments(ctx, bson.M{"status": TaskProcessing})
	assertEqual(t, nil, err)
	assertEqual(t, int64(1), count)

	count, err = coll.CountDocuments(ctx, bson.M{"status": TaskAdded})
	assertEqual(t, nil, err)
	assertEqual(t, int64(0), count)

	err = ws.ResetProcessingTasks()
	assertEqual(t, nil, err)

	count, err = coll.CountDocuments(ctx, bson.M{"status": TaskProcessing})
	assertEqual(t, nil, err)
	assertEqual(t, int64(0), count)

	count, err = coll.CountDocuments(ctx, bson.M{"status": TaskAdded})
	assertEqual(t, nil, err)
	assertEqual(t, int64(1), count)
}
