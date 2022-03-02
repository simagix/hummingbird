// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/simagix/keyhole/mdb"
)

const (
	// MetaDBName defines default meta database name
	MetaDBName = "_neutrino"
	// MetaLogs defines default meta oplogs collection name
	MetaLogs = "logs"
	// MetaOplogs defines default meta oplogs collection name
	MetaOplogs = "oplogs"
	// MetaTasks defines default meta tasks collection name
	MetaTasks = "tasks"
)

// Workspace stores meta database
type Workspace struct {
	dbName  string
	dbURI   string
	staging string
}

// DropMetaDB drops meta database
func (ws *Workspace) DropMetaDB() error {
	if ws.dbURI == "" || ws.dbName == "" {
		return fmt.Errorf("db %v is nil", ws.dbName)
	}
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	return client.Database(ws.dbName).Drop(context.Background())
}

// CleanUpWorkspace removes all cached file
func (ws *Workspace) CleanUpWorkspace() error {
	if ws.staging == "" {
		return fmt.Errorf("workspace staging is not defined")
	}
	var err error
	var filenames []string
	filepath.WalkDir(ws.staging, func(s string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(d.Name(), GZippedBSONFileExt) {
			filenames = append(filenames, s)
		}
		return nil
	})
	for _, filename := range filenames {
		if err = os.Remove(filename); err != nil {
			return fmt.Errorf("os.Remove failed: %v", err)
		}
	}
	return nil
}

// Reset drops meta database and clean up workspace
func (ws *Workspace) Reset() error {
	var err error
	if err = ws.DropMetaDB(); err != nil {
		return fmt.Errorf("DropMetaDB failed: %v", err)
	}
	if err = ws.CreateTaskIndexes(); err != nil {
		return fmt.Errorf("CreateTaskIndexes failed: %v", err)
	}
	return ws.CleanUpWorkspace()
}

// LogConfig records configs
func (ws *Workspace) LogConfig() error {
	inst := GetMigratorInstance()
	ws.Log(fullVersion)
	ws.Log("from " + RedactedURI(inst.Source))
	client, err := GetMongoClient(inst.Source)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	ws.Log("from " + inst.sourceStats.GetClusterShortSummary(client))

	ws.Log("to " + RedactedURI(inst.Target))
	client, err = GetMongoClient(inst.Target)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	ws.Log("to " + inst.targetStats.GetClusterShortSummary(client))
	return nil
}

// CreateTaskIndexes create indexes on tasks collection
func (ws *Workspace) CreateTaskIndexes() error {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	coll := client.Database(ws.dbName).Collection(MetaTasks)
	indexView := coll.Indexes()
	models := []mongo.IndexModel{}
	models = append(models, mongo.IndexModel{Keys: bson.D{{"status", 1}, {"replica_set", 1}, {"_id", 1}}})
	models = append(models, mongo.IndexModel{Keys: bson.D{{"replica_set", 1}, {"parent_id", 1}}})
	_, err = indexView.CreateMany(context.Background(), models)
	return err
}

// Log adds a status to status collection
func (ws *Workspace) Log(status string) error {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	doc := bson.M{"_id": time.Now(), "status": status}
	_, err = client.Database(MetaDBName).Collection(MetaLogs).InsertOne(context.Background(), doc)
	return err
}

// InsertTasks inserts tasks to database
func (ws *Workspace) InsertTasks(tasks []*Task) error {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	var docs []interface{}
	for _, task := range tasks {
		data, err := bson.Marshal(task)
		if err != nil {
			return fmt.Errorf("Marshal failed: %v", err)
		}
		var doc bson.D
		if err = bson.Unmarshal(data, &doc); err != nil {
			return fmt.Errorf("Unmarshal failed: %v", err)
		}
		docs = append(docs, doc)
	}
	_, err = client.Database(MetaDBName).Collection(MetaTasks).InsertMany(context.Background(), docs)
	return err
}

// UpdateTask updates task
func (ws *Workspace) UpdateTask(task *Task) error {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	var ctx = context.Background()
	var result *mongo.UpdateResult
	coll := client.Database(MetaDBName).Collection(MetaTasks)
	doc := bson.M{"status": task.Status, "source_counts": task.SourceCounts,
		"begin_time": task.BeginTime, "end_time": task.EndTime, "updated_by": task.UpdatedBy}
	if task.Status == TaskCompleted {
		doc["inserted"] = task.Inserted
	}
	if result, err = coll.UpdateOne(ctx, bson.M{"_id": task.ID}, bson.M{"$set": doc}); err != nil {
		return fmt.Errorf("UpdateOne failed: %v", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf(`no matched task updated: "%v"`, task.ID)
	}
	if task.ParentID == nil || task.Inserted == 0 || task.Status != TaskCompleted { // no parent to update
		return nil
	}
	if result, err = coll.UpdateOne(ctx, bson.M{"_id": task.ParentID},
		bson.M{"$inc": bson.M{"inserted": task.Inserted}}); err != nil {
		return fmt.Errorf("UpdateOne parent %v failed: %v", task.ParentID, err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf(`no matched parent task updated: "%v", %v`, task.ParentID, err)
	}
	return nil
}

// FindNextTaskAndUpdate returns task by replica a set name
func (ws *Workspace) FindNextTaskAndUpdate(replset string, updatedBy string, rev int) (*Task, error) {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return nil, fmt.Errorf("GetMongoClient failed: %v", err)
	}
	var ctx = context.Background()
	var task = Task{}
	filter := bson.D{{"status", TaskAdded}, {"replica_set", replset}}
	if replset == "" {
		filter = bson.D{{"status", TaskAdded}}
	}
	coll := client.Database(MetaDBName).Collection(MetaTasks)
	opts := options.FindOneAndUpdate()
	opts.SetReturnDocument(options.After)
	opts.SetSort(bson.D{{"replica_set", 1}, {"parent_id", rev}})
	updates := bson.M{"$set": bson.M{"status": TaskProcessing, "begin_time": time.Now(), "updated_by": updatedBy}}
	if err = coll.FindOneAndUpdate(ctx, filter, updates, opts).Decode(&task); err != nil {
		return nil, err
	}
	return &task, nil
}

// CountAllStatus returns task
func (ws *Workspace) CountAllStatus() (TaskStatusCounts, error) {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return TaskStatusCounts{}, fmt.Errorf("GetMongoClient failed: %v", err)
	}
	var counts TaskStatusCounts
	ctx := context.Background()
	pipeline := `[
		{
			"$sort": { "status": 1 }
		}, {
			"$group": {
				"_id": "$status", 
				"count": { "$sum": 1 }
			}
		}
	]`
	coll := client.Database(MetaDBName).Collection(MetaTasks)
	optsAgg := options.Aggregate().SetAllowDiskUse(true)
	var cursor *mongo.Cursor
	if cursor, err = coll.Aggregate(ctx, mdb.MongoPipeline(pipeline), optsAgg); err != nil {
		return counts, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			continue
		}
		if doc["_id"] == TaskAdded {
			counts.Added = ToInt32(doc["count"])
		} else if doc["_id"] == TaskCompleted {
			counts.Completed = ToInt32(doc["count"])
		} else if doc["_id"] == TaskFailed {
			counts.Failed = ToInt32(doc["count"])
		} else if doc["_id"] == TaskProcessing {
			counts.Processing = ToInt32(doc["count"])
		} else if doc["_id"] == TaskSplitting {
			counts.Splitting = ToInt32(doc["count"])
		}
	}
	return counts, err
}

// ResetLongRunningTasks resets long running processing to added
func (ws *Workspace) ResetLongRunningTasks(ago time.Duration) (int, error) {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return 0, fmt.Errorf("GetMongoClient failed: %v", err)
	}
	if ago >= 0 {
		return 0, fmt.Errorf("invlidate past time %v, should be negative", ago)
	}
	ctx := context.Background()
	coll := client.Database(MetaDBName).Collection(MetaTasks)
	updates := bson.M{"$set": bson.M{"status": TaskAdded, "begin_time": time.Time{}, "updated_by": "maid"}}
	filter := bson.D{{"status", TaskProcessing}, {"begin_time", bson.M{"$lt": time.Now().Add(ago)}}}
	result, err := coll.UpdateMany(ctx, filter, updates)
	return int(result.ModifiedCount), err
}

// SaveOplogTimestamp updates timestamp of a shard/replica
func (ws *Workspace) SaveOplogTimestamp(setName string, ts primitive.Timestamp) error {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	filter := bson.M{"_id": setName}
	update := bson.M{"$set": bson.M{"ts": ts}}
	opts := options.Update()
	opts.SetUpsert(true)
	coll := client.Database(MetaDBName).Collection(MetaOplogs)
	_, err = coll.UpdateOne(context.Background(), filter, update, opts)
	return err
}

// GetOplogTimestamp returns timestamp of a shard/replica
func (ws *Workspace) GetOplogTimestamp(setName string) *primitive.Timestamp {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return nil
	}
	filter := bson.M{"_id": setName}
	coll := client.Database(MetaDBName).Collection(MetaOplogs)
	var doc bson.M
	opts := options.FindOne()
	opts.SetProjection(bson.M{"ts": 1, "_id": 0})
	if err = coll.FindOne(context.Background(), filter).Decode(&doc); err != nil {
		return nil
	}
	ts, ok := doc["ts"].(primitive.Timestamp)
	if ok {
		return &ts
	}
	return nil
}

// FindAllParentTasks returns task by replica a set name
func (ws *Workspace) FindAllParentTasks() ([]*Task, error) {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return nil, fmt.Errorf("GetMongoClient failed: %v", err)
	}
	ctx := context.Background()
	var tasks = []*Task{}
	filter := bson.D{{"parent_id", nil}}
	coll := client.Database(MetaDBName).Collection(MetaTasks)
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find %v failed: %v", filter, err)
	}
	for cursor.Next(ctx) {
		var task Task
		bson.Unmarshal(cursor.Current, &task)
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

// ResetParentTask resets and deletes all child tasks
func (ws *Workspace) ResetParentTask(task Task) error {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	ctx := context.Background()
	coll := client.Database(MetaDBName).Collection(MetaTasks)
	_, err = coll.UpdateOne(ctx, bson.M{"_id": task.ID}, bson.M{"$set": bson.M{"status": TaskAdded}})
	if err != nil {
		return fmt.Errorf("UpdateOne failed: %v", err)
	}
	_, err = coll.DeleteMany(ctx, bson.M{"parent_id": task.ID})
	if err != nil {
		return fmt.Errorf("DeleteMany failed: %v", err)
	}
	return nil
}

// ResetProcessingTasks resets processing status to added
func (ws *Workspace) ResetProcessingTasks() error {
	client, err := GetMongoClient(ws.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	ctx := context.Background()
	coll := client.Database(MetaDBName).Collection(MetaTasks)
	_, err = coll.UpdateMany(ctx, bson.M{"status": TaskProcessing}, bson.M{"$set": bson.M{"status": TaskAdded}})
	if err != nil {
		return fmt.Errorf("UpdateMany failed: %v", err)
	}
	return nil
}
