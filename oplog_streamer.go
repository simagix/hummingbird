// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	// CacheIndexFileExt is .index
	CacheIndexFileExt = ".index"
	// GZippedBSONFileExt is .bson.gz
	GZippedBSONFileExt = ".bson.gz"
	// OplogBatchSize set to 10,000
	OplogBatchSize = 10000
)

// OplogStreamer tails oplogs
type OplogStreamer struct {
	SetName string
	Staging string
	URI     string

	isCache bool
	mutex   sync.Mutex
}

// Oplog stores an oplog
type Oplog struct {
	Hash      *int64              `json:"h" bson:"h"`
	Namespace string              `json:"ns" bson:"ns"`
	Object    bson.D              `json:"o" bson:"o"`
	Operation string              `json:"op" bson:"op"`
	Query     bson.D              `json:"o2,omitempty" bson:"o2,omitempty"`
	Term      *int64              `json:"t" bson:"t"`
	Timestamp primitive.Timestamp `json:"ts" bson:"ts"`
	Version   int                 `json:"v" bson:"v"`

	ToDB   string
	ToColl string
}

// OplogStreamers copies oplogs from source to target
func OplogStreamers() error {
	logger := gox.GetLogger("OplogStreamer")
	logger.Remark("stream oplogs")
	inst := GetMigratorInstance()
	for setName, replica := range inst.Replicas() {
		logger.Infof("stream %v (%v)", setName, RedactedURI(replica))
		streamer := OplogStreamer{SetName: setName, Staging: inst.Workspace().staging,
			URI: replica, isCache: true}
		go func() {
			err := streamer.CacheOplogs()
			if err != nil {
				log.Fatal(err)
			}
		}()
		inst.AddOplogStreamer(&streamer)
	}
	return nil
}

// IsCache returns isCache
func (p *OplogStreamer) IsCache() bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.isCache
}

// LiveStream begin applying oplogs to target
func (p *OplogStreamer) LiveStream() {
	go func() {
		p.ApplyCachedOplogs()
		p.mutex.Lock()
		p.isCache = false
		p.mutex.Unlock()
	}()
}

// CacheOplogs store oplogs in files
func (p *OplogStreamer) CacheOplogs() error {
	logger := gox.GetLogger("CacheOplogs")
	logger.Infof("cache oplog from %v", p.SetName)
	os.Mkdir(p.Staging, 0755)
	filename := fmt.Sprintf("%v/%v%v", p.Staging, p.SetName, CacheIndexFileExt)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open file %v to write failed: %v", filename, err)
	}
	defer file.Close()
	ts := primitive.Timestamp{T: uint32(time.Now().Unix())}
	client, err := GetMongoClient(p.URI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	cursor, err := GetTailableCursor(client, ts)
	if err != nil {
		return fmt.Errorf("error finding oplog from %v since %v: %v", p.SetName,
			time.Unix(int64(ts.T), 0).Format(time.RFC3339), err)
	}
	ctx := context.Background()
	for p.IsCache() {
		var oplog Oplog
		if !cursor.TryNext(ctx) {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err = cursor.Decode(&oplog); err != nil {
			continue
		}
		if oplog.Namespace == "" || SkipOplog(oplog) {
			continue
		}
	}
	if err = p.LiveStreamOplogs(); err != nil {
		return fmt.Errorf("oplogs live streaming failed: %v", err)
	}
	return nil
}

// ApplyCachedOplogs applies cached oplogs to target
func (p *OplogStreamer) ApplyCachedOplogs() error {
	logger := gox.GetLogger("CacheOplogs")
	logger.Infof("%v apply cached oplogs", p.SetName)
	return nil
}

// LiveStreamOplogs stream and apply oplogs
func (p *OplogStreamer) LiveStreamOplogs() error {
	logger := gox.GetLogger("LiveStream")
	logger.Infof("%v live stream oplogs", p.SetName)
	return nil
}
