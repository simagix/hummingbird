// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	// BSONSizeLimit set to 10,000
	BSONSizeLimit = 16 * mb
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

	filename string
	isCache  bool
	mutex    sync.Mutex
}

// Oplog stores an oplog
type Oplog struct {
	Hash      *int64              `bson:"h"`
	Namespace string              `bson:"ns"`
	Object    bson.D              `bson:"o"`
	Operation string              `bson:"op"`
	Query     bson.D              `bson:"o2,omitempty"`
	Term      *int64              `bson:"t"`
	Timestamp primitive.Timestamp `bson:"ts"`
	Version   int                 `bson:"v"`
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
		streamer.filename = fmt.Sprintf("%v/%v%v", streamer.Staging, setName, CacheIndexFileExt)
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
	file, err := os.OpenFile(p.filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open file %v to write failed: %v", p.filename, err)
	}
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
	var raws bson.Raw
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
		if len(raws)+len(cursor.Current) > BSONSizeLimit {
			ofile := fmt.Sprintf(`%v/%v.%v.bson.gz`, p.Staging, p.SetName, GetDateTime())
			file.WriteString(ofile + "\n")
			file.Sync()
			if err = gox.OutputGzipped(raws, ofile); err != nil {
				return fmt.Errorf("OutputGzipped %v failed: %v", ofile, err)
			}
			raws = nil
		}
		raws = append(raws, cursor.Current...)
	}
	if len(raws) > 0 {
		reader := bytes.NewReader(raws)
		breader := &BSONReader{Stream: io.NopCloser(reader)}
		var oplogs []Oplog
		var data []byte
		for {
			if data = breader.Next(); data == nil {
				break
			}
			var oplog Oplog
			err = bson.Unmarshal(data, &oplog)
			oplogs = append(oplogs, oplog)
			if len(oplogs) >= MaxBatchSize {
				if err = BulkWriteOplogs(oplogs); err != nil {
					logger.Errorf("BulkWriteOplogs failed: %v", err)
				}
				oplogs = nil
			}
		}
		if len(oplogs) > 0 {
			if err = BulkWriteOplogs(oplogs); err != nil {
				logger.Errorf("BulkWriteOplogs failed: %v", err)
			}
		}
	}
	file.Close()
	if err = p.LiveStreamOplogs(cursor); err != nil {
		return fmt.Errorf("oplogs live streaming failed: %v", err)
	}
	return nil
}

// ApplyCachedOplogs applies cached oplogs to target
func (p *OplogStreamer) ApplyCachedOplogs() error {
	logger := gox.GetLogger("CacheOplogs")
	logger.Infof("%v reading %v", p.SetName, p.filename)
	file, err := os.OpenFile(p.filename, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file %v failed: %v", p.filename, err)
	}
	defer file.Close()
	var reader *bufio.Reader
	if reader, err = gox.NewFileReader(file.Name()); err == nil {
		for {
			data, _, err := reader.ReadLine()
			if err != nil { // 0x0A separator = newline
				break
			}
			infile := string(data)
			logger.Infof("%v apply oplogs from %v", p.SetName, infile)
			breader, err := NewBSONReader(infile)
			if err != nil {
				return fmt.Errorf("read %v failed", err)
			}
			var oplogs []Oplog
			for {
				if data = breader.Next(); data == nil {
					break
				}
				var oplog Oplog
				err = bson.Unmarshal(data, &oplog)
				oplogs = append(oplogs, oplog)
				if len(oplogs) >= MaxBatchSize {
					if err = BulkWriteOplogs(oplogs); err != nil {
						logger.Errorf("BulkWriteOplogs failed: %v", err)
					}
					oplogs = nil
				}
			}
			if len(oplogs) > 0 {
				if err = BulkWriteOplogs(oplogs); err != nil {
					logger.Errorf("BulkWriteOplogs failed: %v", err)
				}
				oplogs = nil
			}
			time.Sleep(50 * time.Millisecond) // yield
		}
	}
	return nil
}

// LiveStreamOplogs stream and apply oplogs
func (p *OplogStreamer) LiveStreamOplogs(cursor *mongo.Cursor) error {
	var err error
	logger := gox.GetLogger("LiveStreamOplogs")
	ctx := context.Background()
	logger.Infof("%v live stream oplogs", p.SetName)
	var oplogs []Oplog
	last := time.Now()
	for {
		var oplog Oplog
		if !cursor.TryNext(ctx) {
			if len(oplogs) > 0 {
				if err = BulkWriteOplogs(oplogs); err != nil {
					logger.Errorf("BulkWriteOplogs failed: %v", err)
				}
				oplogs = nil
			}
			if time.Since(last) > 10*time.Second {
				last = time.Now()
				logger.Infof("%v: lag 0", p.SetName)
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err = cursor.Decode(&oplog); err != nil {
			continue
		}
		if oplog.Namespace == "" || SkipOplog(oplog) {
			continue
		}
		oplogs = append(oplogs, oplog)
		if len(oplogs) >= MaxBatchSize || time.Since(last) > 10*time.Second {
			last = time.Now()
			if len(oplogs) == 0 {
				logger.Infof("%v: lag 0", p.SetName)
				continue
			}
			if err = BulkWriteOplogs(oplogs); err != nil {
				logger.Errorf("BulkWriteOplogs failed: %v", err)
			}
			lag := time.Since(time.Unix(int64(oplogs[len(oplogs)-1].Timestamp.T), 0)).Truncate(time.Second)
			logger.Infof("%v: lag %v", p.SetName, lag)
			oplogs = nil
		}
	}
	return nil
}
