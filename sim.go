// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/simagix/keyhole/mdb"

	"github.com/simagix/gox"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/uuid"
)

const (
	// DefaultDuration  s.duration to simulate
	DefaultDuration = 5 * time.Minute
	// DefaultNumOplogs default number of oplogs per thread
	DefaultNumOplogs = 300
)

var (
	// Rainbow colors
	Rainbow = []string{"Red", "Orange", "Yellow", "Green", "Blue", "Indigo", "Violet"}

	span = 500
)

// Simulator stores simulation info
type Simulator struct {
	Namespaces []string `bson:"namespaces"`
	Threads    struct {
		Find   int `bson:"find"`
		Insert int `bson:"insert"`
		Write  int `bson:"write"`
	} `bson:"threads"`
	Seconds   int    `bson:"seconds_to_run"`
	NumOplogs int    `bson:"oplogs_per_second"`
	URI       string `bson:"uri"`
	Verbose   bool   `bson:"verbose"`

	client   *mongo.Client
	duration time.Duration
	ids      []interface{}
	mutex    sync.Mutex
}

// Simulate simulates 2 inserts, 1 update/delete, and 1 find
func Simulate(filename string) error {
	var sim Simulator
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	if err = bson.UnmarshalExtJSON(data, false, &sim); err != nil {
		return err
	}
	client, err := GetMongoClient(sim.URI)
	if err != nil {
		return fmt.Errorf("GetMongoClient failed: %v", err)
	}
	sim.client = client
	if sim.NumOplogs == 0 {
		sim.NumOplogs = DefaultNumOplogs
	}
	sim.duration = time.Duration(sim.Seconds) * time.Second
	if sim.Seconds == 0 {
		sim.duration = DefaultDuration
	}
	return StartSimulation(sim)
}

// StartSimulation starts a simulation
func StartSimulation(sim Simulator) error {
	if sim.Verbose {
		logger := gox.GetLogger("simulator")
		logger.SetLoggerLevel(gox.Debug)
	}
	wg := gox.NewWaitGroup(sim.Threads.Find + sim.Threads.Insert + sim.Threads.Write)

	for i := 0; i < sim.Threads.Insert; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sim.Insert(); err != nil {
				log.Fatal(err)
			}
		}()
	}

	for i := 0; i < sim.Threads.Write; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sim.Modify(); err != nil {
				log.Fatal(err)
			}
		}()
	}

	for i := 0; i < sim.Threads.Find; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sim.Find(); err != nil {
				log.Fatal(err)
			}
		}()
	}

	wg.Wait()
	return nil
}

// Insert simulates insertions
func (s *Simulator) Insert() error {
	logger := gox.GetLogger()
	ctx := context.Background()
	size := len(s.Namespaces)
	colls := make([]*mongo.Collection, size)
	for i, ns := range s.Namespaces {
		dbName, collName := mdb.SplitNamespace(ns)
		colls[i] = s.client.Database(dbName).Collection(collName)
	}
	exch := 0
	seq := 0
	begin := time.Now()
	for time.Since(begin) < s.duration {
		now := time.Now()
		exch++
		exch = exch % size
		coll := colls[exch]

		seq++
		doc := DocGen(seq)
		t := time.Now()
		if _, err := coll.InsertOne(ctx, doc); err != nil {
			return fmt.Errorf("InsertOne failed: %v", err)
		}
		logger.Debugf("InsertOne  took %v", time.Since(t))

		docs := []interface{}{}
		for i := 0; i < s.NumOplogs-1; i++ {
			seq++
			doc := DocGen(seq)
			docs = append(docs, doc)
		}
		t = time.Now()
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			return fmt.Errorf("InsertMany failed: %v", err)
		}
		logger.Debugf("InsertMany took %v (size=%v)", time.Since(t), len(docs))

		s.mutex.Lock()
		for _, doc := range docs {
			s.ids = append(s.ids, doc.(bson.D).Map()["_id"])
		}
		size := len(s.ids)
		if len(s.ids) > s.NumOplogs+span {
			s.ids = s.ids[size-span:]
		}
		s.mutex.Unlock()
		pauseRemainedSecond(time.Since(now))
	}
	return nil
}

// Modify simulates updates and deletions
func (s *Simulator) Modify() error {
	logger := gox.GetLogger()
	ctx := context.Background()
	size := len(s.Namespaces)
	colls := make([]*mongo.Collection, size)
	for i, ns := range s.Namespaces {
		dbName, collName := mdb.SplitNamespace(ns)
		colls[i] = s.client.Database(dbName).Collection(collName)
	}
	exch := 0
	begin := time.Now()
	for time.Since(begin) < s.duration {
		now := time.Now()
		exch++
		exch = exch % size
		coll := colls[exch]
		if len(s.ids) < s.NumOplogs {
			continue
		}

		s.mutex.Lock()
		samples := s.NumOplogs / 2
		ids := make([]interface{}, len(s.ids[:samples]))
		copy(ids, s.ids[:samples])
		s.mutex.Unlock()

		t := time.Now()
		query := bson.D{{"_id", ids[0]}}
		update := bson.D{{"$inc", bson.D{{"int64", 1}}}}
		if _, err := coll.UpdateOne(ctx, query, update); err != nil {
			return fmt.Errorf("UpdateMany failed: %v", err)
		}
		logger.Debugf("UpdateOne  took %v", time.Since(t))

		filter := bson.D{{"_id", bson.D{{"$in", ids}}}}
		t = time.Now()
		if _, err := coll.UpdateMany(ctx, filter, update); err != nil {
			return fmt.Errorf("UpdateMany failed: %v", err)
		}
		logger.Debugf("UpdateMany took %v (size=%v)", time.Since(t), len(ids))

		s.mutex.Lock()
		ids = make([]interface{}, len(s.ids[samples:]))
		copy(ids, s.ids[samples:])
		s.mutex.Unlock()

		t = time.Now()
		if _, err := coll.DeleteOne(ctx, query); err != nil {
			return fmt.Errorf("DeleteOne failed: %v", err)
		}
		logger.Debugf("DeleteOne  took %v", time.Since(t))

		filter = bson.D{{"_id", bson.D{{"$in", ids}}}}
		t = time.Now()
		if _, err := coll.DeleteMany(ctx, filter); err != nil {
			return fmt.Errorf("DeleteMany failed: %v", err)
		}
		logger.Debugf("DeleteMany took %v (size=%v)", time.Since(t), len(ids))
		pauseRemainedSecond(time.Since(now))
	}
	return nil
}

// Find simulates finds
func (s *Simulator) Find() error {
	logger := gox.GetLogger()
	pipeline := `[
		{ $sample: { size: 3629 } },
		{ "$group": { "_id": "$color", "total": { "$sum": 1 } } }
	]`
	ctx := context.Background()
	size := len(s.Namespaces)
	colls := make([]*mongo.Collection, size)
	for i, ns := range s.Namespaces {
		dbName, collName := mdb.SplitNamespace(ns)
		colls[i] = s.client.Database(dbName).Collection(collName)
	}
	exch := 0
	begin := time.Now()
	for time.Since(begin) < s.duration {
		now := time.Now()
		exch++
		exch = exch % size
		coll := colls[exch]
		if len(s.ids) < s.NumOplogs {
			continue
		}

		s.mutex.Lock()
		samples := s.NumOplogs / 2
		if samples > 102 {
			samples = 102
		}
		ids := make([]interface{}, len(s.ids[:samples]))
		copy(ids, s.ids[:samples])
		s.mutex.Unlock()
		filter := bson.D{{"_id", bson.D{{"$in", ids}}}}
		t := time.Now()
		cursor, err := coll.Find(ctx, filter)
		if err != nil {
			return fmt.Errorf("Find failed: %v", err)
		}
		logger.Debugf("Find       took %v (size=%v)", time.Since(t), len(ids))
		cursor.Close(ctx)

		opts := options.Aggregate().SetAllowDiskUse(true)
		t = time.Now()
		if cursor, err = coll.Aggregate(ctx, mdb.MongoPipeline(pipeline), opts); err != nil {
			return fmt.Errorf("Aggregate failed: %v", err)
		}
		logger.Debugf("Aggregate  took %v ($group)", time.Since(t))
		cursor.Close(ctx)
		pauseRemainedSecond(time.Since(now))
	}
	return nil
}

func pauseRemainedSecond(elapsed time.Duration) {
	if elapsed < time.Second {
		dur := time.Duration(1000-elapsed.Milliseconds()) * time.Millisecond
		time.Sleep(dur)
	} else {
		time.Sleep(10 * time.Millisecond)
	}
}

// DocGen returns a bson doc
func DocGen(i int) bson.D {
	n := i + 1001
	num := n*n + Port
	doc := bson.D{{"_id", primitive.NewObjectID()},
		{"color", Rainbow[i%len(Rainbow)]},
		{"float64", float64(num)},
		{"int64", int64(num)},
		{"seq", i},
		{"string", fmt.Sprintf("%06d-%v-%v-%v", i+1, num, n, num)},
		{"ts", time.Now()},
	}
	var list []int
	for n := 101; n < 110; n++ {
		list = append(list, n*n-n)
	}
	doc = append(doc, bson.D{{"array", list}}...)
	doc = append(doc, bson.D{{"subdoc", bson.D{{"level1", doc}}}}...)
	doc = append(doc, bson.D{{"filler", fmt.Sprintf("%v%v", n, LogoPNG[:2500])}}...)
	auuid, err := uuid.New()
	if err != nil {
		return doc
	}
	doc = append(doc, bson.E{"binary", auuid})
	doc = append(doc, bson.E{"bin1", primitive.Binary{Subtype: byte(1), Data: []byte(auuid[:])}})
	doc = append(doc, bson.E{"bin2", primitive.Binary{Subtype: byte(2), Data: []byte(auuid[:])}})
	doc = append(doc, bson.E{"bin3", primitive.Binary{Subtype: byte(3), Data: []byte(auuid[:])}})
	doc = append(doc, bson.E{"uuid", primitive.Binary{Subtype: byte(4), Data: []byte(auuid[:])}})
	return doc
}

// DataGen populate data
func DataGen(coll *mongo.Collection, total int) (*mongo.InsertManyResult, error) {
	ctx := context.Background()
	coll.Drop(ctx)
	var docs []interface{}
	for i := 0; i < total; i++ {
		docs = append(docs, DocGen(i))
	}
	return coll.InsertMany(ctx, docs)
}

// DataGenMulti populate data into different collections
func DataGenMulti(db *mongo.Database, total int, nColls int) error {
	ctx := context.Background()
	db.Drop(ctx)
	docs := map[int][]interface{}{}
	for i := 0; i < total; i++ {
		docs[i%nColls] = append(docs[i%nColls], DocGen(i))
	}
	var colls []*mongo.Collection
	colls = make([]*mongo.Collection, 3)
	for i := 0; i < len(colls); i++ {
		colls[i] = db.Collection(fmt.Sprintf("datagen_%v", i))
		colls[i].InsertMany(ctx, docs[i])

		var session mongo.Session
		var update = bson.M{"$set": bson.M{"birth_year": 1963}}
		client := db.Client()
		var err error
		if session, err = client.StartSession(); err != nil {
			return err
		}
		id1 := bson.D{{"_id", primitive.NewObjectID()}, {"tag", 1}}
		id2 := bson.D{{"_id", primitive.NewObjectID()}, {"tag", 3}}
		ids := []interface{}{id1, id2}
		colls[i].InsertMany(context.Background(), ids)
		if err = session.StartTransaction(); err != nil {
			return err
		}
		if err = mongo.WithSession(ctx, session, func(sc mongo.SessionContext) error {
			if _, err = colls[i].UpdateOne(sc, id1, update); err != nil {
				return err
			}
			if _, err = colls[i].UpdateOne(sc, id2, update); err != nil {
				return err
			}
			if _, err = colls[i].DeleteOne(sc, bson.M{"tag": 1}); err != nil {
				return err
			}
			if _, err = colls[i].DeleteMany(sc, bson.M{"tag": 3}); err != nil {
				return err
			}
			if err = session.CommitTransaction(sc); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		session.EndSession(ctx)

	}
	return nil
}
