// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/simagix/keyhole/mdb"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// NumberSplitters number of splitters
	NumberSplitters = 4
)

// Splitter splits collection into small tasks
func Splitter(tasks []*Task) error {
	now := time.Now()
	logger := gox.GetLogger("Splitter")
	inst := GetMigratorInstance()
	wg := gox.NewWaitGroup(NumberSplitters)
	for _, task := range tasks {
		if task.Status == TaskCompleted {
			continue
		}
		wg.Add(1)
		client, err := GetMongoClient(inst.Replicas()[task.SetName])
		if err != nil {
			return fmt.Errorf("GetMongoClient failed: %v", err)
		}
		go func(client *mongo.Client, task *Task) {
			defer wg.Done()
			splitTask(client, task)
		}(client, task)
	}
	wg.Wait()
	logger.Infof("collections split, took %v", time.Since(now))
	return nil
}

func splitTask(client *mongo.Client, task *Task) error {
	inst := GetMigratorInstance()
	ctx := context.Background()
	dbName, collName := mdb.SplitNamespace(task.Namespace)
	opts := options.Find()
	opts.SetProjection(bson.D{{"_id", 1}})
	opts.SetSort(bson.D{{"_id", 1}})
	query := bson.D{}
	if len(task.Include.Filter) > 0 {
		query = task.Include.Filter
	}
	cursor, err := client.Database(dbName).Collection(collName).Find(ctx, query, opts)
	if err != nil {
		log.Fatalf("splitTask Find failed: %v", err)
	}
	defer cursor.Close(ctx)
	task.BeginTime = time.Now()
	task.Status = TaskSplitting
	ws := inst.Workspace()
	ws.UpdateTask(task)
	total := 0
	count := 0
	var first bson.M
	parentID := task.ID
	var last bson.M
	for cursor.Next(ctx) {
		total++
		count++
		if count == 1 {
			cursor.Decode(&first)
		} else if count == inst.Block {
			cursor.Decode(&last)
			subTask := &Task{ID: primitive.NewObjectID(), IDs: []interface{}{first["_id"], last["_id"]},
				Namespace: task.Namespace, ParentID: &parentID, SetName: task.SetName, Status: TaskAdded,
				Include: task.Include, SourceCounts: count, UpdatedBy: "splitter"}
			ws.InsertTasks([]*Task{subTask})
			count = 0
			first = nil
			last = nil
		} else if cursor.RemainingBatchLength() == 0 {
			cursor.Decode(&last)
		}
	}
	if first != nil {
		if count == 1 {
			last = first
		}
		subTask := &Task{ID: primitive.NewObjectID(), IDs: []interface{}{first["_id"], last["_id"]},
			Namespace: task.Namespace, ParentID: &parentID, SetName: task.SetName, Status: TaskAdded,
			Include: task.Include, SourceCounts: count, UpdatedBy: "splitter"}
		ws.InsertTasks([]*Task{subTask})
	}
	task.EndTime = time.Now()
	task.Status = TaskCompleted
	task.SourceCounts = total
	ws.UpdateTask(task)
	return nil
}
