// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"time"

	"github.com/simagix/keyhole/mdb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	kb = 1024
	mb = kb * kb
	gb = kb * mb
	tb = kb * gb

	// MaxBatchDataSize size of a insert batch
	MaxBatchDataSize = (64 * mb)
	// MaxBatchSize size of a insert batch
	MaxBatchSize = 1000
)

// Task holds migration task information
type Task struct {
	BeginTime    time.Time           `bson:"begin_time"`
	EndTime      time.Time           `bson:"end_time"`
	ID           primitive.ObjectID  `bson:"_id"`
	IDs          []interface{}       `bson:"ids"`
	Include      Include             `bson:"include"`
	Inserted     int                 `bson:"inserted"`
	Namespace    string              `bson:"ns"`
	ParentID     *primitive.ObjectID `bson:"parent_id"`
	SetName      string              `bson:"replica_set"`
	SourceCounts int                 `bson:"source_counts"`
	Status       string              `bson:"status"`
	UpdatedBy    string              `bson:"updated_by"`
}

// CopyData copies data
func (p *Task) CopyData(source *mongo.Collection, target *mongo.Collection) error {
	ctx := context.Background()
	if p.SourceCounts == 0 {
		return nil
	}
	if len(p.IDs) < 2 {
		return fmt.Errorf("no _id range found")
	}
	query := bson.D{{"_id", bson.D{{"$gte", p.IDs[0]}}}, {"_id", bson.D{{"$lte", p.IDs[1]}}}}
	if len(p.Include.Filter) > 0 {
		query = append(p.Include.Filter, query...)
	}
	opts := options.Find()
	cursor, err := source.Find(ctx, query, opts)
	if err != nil {
		return fmt.Errorf("CopyData Find failed: %v", err)
	}
	defer cursor.Close(ctx)
	size := 0
	var docs []interface{}
	for cursor.Next(ctx) {
		if len(docs) >= 1000 || size > MaxBatchDataSize {
			p.batchedCopy(target, docs)
			size = 0
			docs = []interface{}{}
		}
		doc := make(bson.Raw, len(cursor.Current))
		copy(doc, cursor.Current)
		docs = append(docs, doc)
		size += len(cursor.Current)
	}
	if len(docs) > 0 {
		p.batchedCopy(target, docs)
	}
	return nil
}

func (p *Task) batchedCopy(target *mongo.Collection, docs []interface{}) {
	ctx := context.Background()
	opts := options.InsertMany()
	opts.SetOrdered(false)
	result, err := target.InsertMany(ctx, docs, opts)
	if err != nil && (mdb.IsDuplicateKeyError(err) || mdb.GetErrorCode(err) == 16755) {
		var ids []interface{}
		for _, raw := range docs {
			var doc bson.M
			bson.Unmarshal(raw.(bson.Raw), &doc)
			ids = append(ids, doc["_id"])
		}
		query := bson.D{{"_id", bson.D{{"$in", ids}}}}
		cnt, _ := target.CountDocuments(ctx, query)
		if int(cnt) == len(docs) {
			p.Inserted += int(cnt)
		} else {
			fmt.Println("TODO")
		}
	} else if result != nil {
		p.Inserted += len(result.InsertedIDs)
	}
}
