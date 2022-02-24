// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Task holds migration task information
type Task struct {
	BeginTime    time.Time           `bson:"begin_time"`
	EndTime      time.Time           `bson:"end_time"`
	ID           primitive.ObjectID  `bson:"_id"`
	IDs          []interface{}       `bson:"ids"`
	Include      Include             `bson:"query_filter"`
	Inserted     int                 `bson:"inserted"`
	Namespace    string              `bson:"ns"`
	ParentID     *primitive.ObjectID `bson:"parent_id"`
	SetName      string              `bson:"replica_set"`
	SourceCounts int                 `bson:"source_counts"`
	Status       string              `bson:"status"`
	UpdatedBy    string              `bson:"updated_by"`
}
