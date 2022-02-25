// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// None is an empty string
	None = ""
	// Tasks tasks
	Tasks = "tasks"
	// TaskAdded added
	TaskAdded = "added"
	// TaskCompleted completed
	TaskCompleted = "completed"
	// TaskFailed failed
	TaskFailed = "failed"
	// TaskProcessing processing
	TaskProcessing = "processing"
	// TaskSplitting splitting
	TaskSplitting = "splitting"
)

// TaskStatusCounts stores counts of all status
type TaskStatusCounts struct {
	Added      int32
	Completed  int32
	Failed     int32
	Processing int32
	Splitting  int32
}

// DataCopier copies data from source to target
func DataCopier() error {
	var err error
	now := time.Now()
	ctx := context.Background()
	logger := gox.GetLogger("DataCopier")
	inst := GetMigratorInstance()
	logger.Remark("copy data")
	// all qualified collections
	var includes []*Include
	if len(inst.Included()) > 0 {
		for _, include := range inst.Included() {
			includes = append(includes, include)
		}
	} else {
		if includes, err = getQualifiedCollections(inst.Source); err != nil {
			return fmt.Errorf("getQualifiedCollections failed: %v", err)
		}
	}
	tasks := []*Task{}
	sourceClient, err := GetMongoClient(inst.Source)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	for _, uri := range inst.Replicas() {
		cs, err := mdb.ParseURI(uri)
		if err != nil {
			return fmt.Errorf("ParseURI failed: %v", err)
		}
		for _, include := range includes {
			dbName, collName := mdb.SplitNamespace(include.Namespace)
			if collName == "" || collName == "*" { // expand to include all collections
				var cursor *mongo.Cursor
				if cursor, err = sourceClient.Database(dbName).ListCollections(ctx, bson.D{}); err != nil {
					return err
				}
				for cursor.Next(ctx) {
					var doc bson.M
					if err = cursor.Decode(&doc); err != nil {
						return fmt.Errorf("decode failed: %v", err)
					}
					if doc["name"] == nil {
						continue
					}
					collName, ok := doc["name"].(string)
					if !ok || (doc["type"] != "" && doc["type"] != "collection") || strings.HasPrefix(collName, "system.") {
						continue
					}
					ns := fmt.Sprintf("%v.%v", dbName, doc["name"])
					task := &Task{ID: primitive.NewObjectID(), IDs: []interface{}{}, Namespace: ns,
						ParentID: nil, SetName: cs.ReplicaSet, Status: TaskAdded, Include: *include, UpdatedBy: "init"}
					tasks = append(tasks, task)
				}
				cursor.Close(ctx)
				continue
			}
			task := &Task{ID: primitive.NewObjectID(), IDs: []interface{}{}, Namespace: include.Namespace,
				ParentID: nil, SetName: cs.ReplicaSet, Status: TaskAdded, Include: *include, UpdatedBy: "init"}
			tasks = append(tasks, task)
		}
	}
	ws := inst.Workspace()
	ws.InsertTasks(tasks)
	for i := 0; i < inst.Workers; i++ { // start all workers
		go Worker(i + 1)
		time.Sleep(10 * time.Millisecond)
	}
	if err = Splitter(tasks); err != nil {
		return fmt.Errorf("Splitter failed: %v", err)
	}
	if err = Wait(); err != nil {
		return fmt.Errorf("Wait failed: %v", err)
	}
	logger.Infof("data copied, took %v", time.Since(now))
	return nil
}

func getQualifiedCollections(uri string) ([]*Include, error) {
	var includes []*Include
	client, err := GetMongoClient(uri)
	if err != nil {
		return nil, fmt.Errorf("GetMongoClient failed: %v", err)
	}
	namespaces, err := GetQualifiedNamespaces(client, true, MetaDBName)
	if err != nil {
		return includes, fmt.Errorf("GetQualifiedNamespaces failed: %v", err)
	}
	for _, ns := range namespaces {
		include := Include{Namespace: ns, Filter: bson.D{}}
		includes = append(includes, &include)
	}
	return includes, nil
}

// Wait waits for all tasks to be processed
func Wait() error {
	inst := GetMigratorInstance()
	ws := inst.Workspace()
	logger := gox.GetLogger("Wait")
	client, err := GetMongoClient(inst.Target)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	var counts TaskStatusCounts
	btime := time.Now()
	if counts, err = ws.CountAllStatus(client); err != nil {
		return fmt.Errorf(`CountAllStatus failed: %v`, err)
	}
	logger.Infof("added: %v, completed: %v, failed: %v, processing: %v, splitting: %v",
		counts.Added, counts.Completed, counts.Failed, counts.Processing, counts.Splitting)
	count := 0
	for {
		time.Sleep(30 * time.Second)
		if counts, err = ws.CountAllStatus(client); err != nil {
			return fmt.Errorf(`CountAllStatus failed: %v`, err)
		}
		if time.Since(btime) > 1*time.Minute {
			log.Printf("added: %v, completed: %v, failed: %v, processing: %v, splitting: %v",
				counts.Added, counts.Completed, counts.Failed, counts.Processing, counts.Splitting)
			btime = time.Now()
			count++
			if _, err = ws.ResetLongRunningTasks(-10 * time.Minute); err != nil { // reset if a task already lasts 10 mins
				return fmt.Errorf(`AuditLongRunningTasks failed: %v`, err)
			}
		}
		if count > 60 {
			count = 0
			logger.Infof("added: %v, completed: %v, failed: %v, processing: %v, splitting: %v",
				counts.Added, counts.Completed, counts.Failed, counts.Processing, counts.Splitting)
		}
		if (counts.Added + counts.Processing) == 0 {
			return nil
		}
	}
}
