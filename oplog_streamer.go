// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	// BSONSizeLimit set to 10,000
	BSONSizeLimit = 16 * mb
	// CacheDataSizeLimit set to 10,000
	CacheDataSizeLimit = 4 * BSONSizeLimit
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

	cached  string
	isCache bool
	mutex   sync.Mutex
	ts      *primitive.Timestamp
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
		for {
			var err error
			if p.cached, err = p.ApplyCachedOplogs(); err != nil {
				log.Fatalf("ApplyCachedOplogs failed: %v", err)
			}
			if p.cached == "" {
				break
			}
		}
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
	p.ts = &primitive.Timestamp{T: uint32(time.Now().Unix())}
	client, err := GetMongoClient(p.URI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	cursor, err := GetTailableCursor(client, p.ts)
	if err != nil {
		return fmt.Errorf("error finding oplog from %v since %v: %v", p.SetName,
			time.Unix(int64(p.ts.T), 0).Format(time.RFC3339), err)
	}
	ctx := context.Background()
	var raws bson.Raw
	for p.IsCache() {
		var oplog Oplog
		if !cursor.TryNext(ctx) {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		if err = cursor.Decode(&oplog); err != nil {
			continue
		}
		if oplog.Namespace == "" || SkipOplog(oplog) {
			continue
		}
		if len(raws)+len(cursor.Current) > CacheDataSizeLimit {
			ofile := fmt.Sprintf(`%v/%v.%v.bson.gz`, p.Staging, p.SetName, GetDateTime())
			if err = gox.OutputGzipped(raws, ofile); err != nil {
				return fmt.Errorf("OutputGzipped %v failed: %v", ofile, err)
			}
			raws = nil
		}
		raws = append(raws, cursor.Current...)
	}
	if len(raws) > 0 {
		logger.Infof("%v apply oplogs from memory", p.SetName)
		reader := bytes.NewReader(raws)
		breader := &BSONReader{Stream: io.NopCloser(reader)}
		var oplogs []Oplog
		var data []byte
		var op *Oplog
		processed := 0
		for {
			if data = breader.Next(); data == nil {
				break
			}
			processed++
			var oplog Oplog
			err = bson.Unmarshal(data, &oplog)
			op = &oplog
			oplogs = append(oplogs, oplog)
			if len(oplogs) >= MaxBatchSize {
				if _, err := BulkWriteOplogs(oplogs); err != nil {
					logger.Errorf("BulkWriteOplogs failed: %v", err)
				}
				oplogs = nil
			}
		}
		if len(oplogs) > 0 {
			if _, err := BulkWriteOplogs(oplogs); err != nil {
				logger.Errorf("BulkWriteOplogs failed: %v", err)
			}
		}
		lag := time.Since(time.Unix(int64(op.Timestamp.T), 0)).Truncate(time.Second)
		logger.Infof("%v lag %v, %v processed", p.SetName, lag, processed)
		p.ts = &oplogs[len(oplogs)-1].Timestamp
	}
	if err = p.LiveStreamOplogs(p.ts); err != nil {
		return fmt.Errorf("oplogs live streaming failed: %v", err)
	}
	return nil
}

// ApplyCachedOplogs applies cached oplogs to target
func (p *OplogStreamer) ApplyCachedOplogs() (string, error) {
	inst := GetMigratorInstance()
	logger := gox.GetLogger("CacheOplogs")
	filenames := []string{}
	filepath.WalkDir(inst.Workspace().staging, func(s string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(d.Name(), p.SetName) && strings.HasSuffix(d.Name(), GZippedBSONFileExt) {
			if s > p.cached {
				filenames = append(filenames, s)
			}
		}
		return nil
	})
	if len(filenames) == 0 {
		return "", nil
	}
	logger.Infof("%v has %v file(s)", p.SetName, len(filenames))
	sort.Slice(filenames, func(i int, j int) bool {
		return filenames[i] < filenames[j]
	})
	for _, filename := range filenames {
		logger.Infof("%v apply oplogs from %v", p.SetName, filename)
		breader, err := NewBSONReader(filename)
		if err != nil {
			return filename, fmt.Errorf("read %v failed", err)
		}
		var oplogs []Oplog
		var op *Oplog
		processed := 0
		for {
			var data []byte
			if data = breader.Next(); data == nil {
				break
			}
			processed++
			var oplog Oplog
			err = bson.Unmarshal(data, &oplog)
			op = &oplog
			oplogs = append(oplogs, oplog)
			if len(oplogs) >= MaxBatchSize {
				if _, err := BulkWriteOplogs(oplogs); err != nil {
					logger.Errorf("BulkWriteOplogs failed: %v", err)
				}
				oplogs = nil
			}
		}
		if len(oplogs) > 0 {
			if _, err := BulkWriteOplogs(oplogs); err != nil {
				logger.Errorf("BulkWriteOplogs failed: %v", err)
			}
		}
		lag := time.Since(time.Unix(int64(op.Timestamp.T), 0)).Truncate(time.Second)
		logger.Infof("%v lag %v, %v processed", p.SetName, lag, processed)
		p.ts = &oplogs[len(oplogs)-1].Timestamp
		time.Sleep(50 * time.Millisecond) // yield
	}
	return filenames[len(filenames)-1], nil
}

// LiveStreamOplogs stream and apply oplogs
func (p *OplogStreamer) LiveStreamOplogs(ts *primitive.Timestamp) error {
	logger := gox.GetLogger("LiveStreamOplogs")
	ctx := context.Background()
	logger.Infof("%v live stream oplogs", p.SetName)
	client, err := GetMongoClient(p.URI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	cursor, err := GetTailableCursor(client, ts)
	var oplogs []Oplog
	last := time.Now()
	for {
		var oplog Oplog
		if !cursor.TryNext(ctx) {
			if len(oplogs) > 0 {
				if _, err := BulkWriteOplogs(oplogs); err != nil {
					logger.Errorf("BulkWriteOplogs failed: %v", err)
				}
				oplogs = nil
			}
			if time.Since(last) > 10*time.Second {
				last = time.Now()
				logger.Infof("%v lag 0s", p.SetName)
			}
			time.Sleep(1 * time.Millisecond)
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
				logger.Infof("%v lag 0s", p.SetName)
				continue
			}
			if _, err := BulkWriteOplogs(oplogs); err != nil {
				logger.Errorf("BulkWriteOplogs failed: %v", err)
			}
			lag := time.Since(time.Unix(int64(oplogs[len(oplogs)-1].Timestamp.T), 0)).Truncate(time.Second)
			logger.Infof("%v: lag %v", p.SetName, lag)
			oplogs = nil
		}
	}
	return nil
}
