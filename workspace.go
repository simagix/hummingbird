// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/simagix/gox"
)

const (
	// CacheIndexFileExt is .index
	CacheIndexFileExt = ".index"
	// GZippedBSONFileExt is .bson.gz
	GZippedBSONFileExt = ".bson.gz"

	// MetaDBName defines default meta database name
	MetaDBName = "_neutrino"
	// MetaLogCollection defines default meta oplogs collection name
	MetaLogCollection = "logs"
	// MetaOplogCollection defines default meta oplogs collection name
	MetaOplogCollection = "oplogs"
	// MetaTaskCollection defines default meta tasks collection name
	MetaTaskCollection = "tasks"
)

// Workspace stores meta database
type Workspace struct {
	dbName  string
	dbURI   string
	staging string
}

// DropMetaDB drops meta database
func (p *Workspace) DropMetaDB() error {
	if p.dbURI == "" || p.dbName == "" {
		return fmt.Errorf("db %v is nil", p.dbName)
	}
	client, err := GetMongoClient(p.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	return client.Database(p.dbName).Drop(context.Background())
}

// CleanUpWorkspace removes all cached file
func (p *Workspace) CleanUpWorkspace() error {
	if p.staging == "" {
		return fmt.Errorf("workspace staging is not defined")
	}
	var err error
	var filenames []string
	filepath.WalkDir(p.staging, func(s string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(d.Name()) == CacheIndexFileExt {
			filenames = append(filenames, s)
		}
		return nil
	})
	for _, filename := range filenames {
		var reader *bufio.Reader
		if reader, err = gox.NewFileReader(filename); err != nil {
			return err
		}
		for {
			var buf []byte
			if buf, _, err = reader.ReadLine(); err != nil { // 0x0A separator = newline
				break
			}
			if err = os.Remove(string(buf)); err != nil {
				return err
			}
		}
		if err = os.Remove(filename); err != nil {
			return err
		}
	}
	return nil
}

// Reset drops meta database and clean up workspace
func (p *Workspace) Reset() error {
	var err error
	if err = p.DropMetaDB(); err != nil {
		return err
	}
	return p.CleanUpWorkspace()
}

// InsertTasks inserts tasks to database
func (p *Workspace) InsertTasks(tasks []*Task) error {
	client, err := GetMongoClient(p.dbURI)
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
	client.Database(MetaDBName).Collection(Tasks).InsertMany(context.Background(), docs)
	return nil
}

// UpdateTask updates task
func (p *Workspace) UpdateTask(task *Task) error {
	client, err := GetMongoClient(p.dbURI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	var ctx = context.Background()
	var result *mongo.UpdateResult
	coll := client.Database(MetaDBName).Collection(Tasks)
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
