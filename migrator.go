// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/simagix/keyhole/mdb"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Migrator stores migration configurations
type Migrator struct {
	Block    int      `bson:"block,omitempty"`
	Command  string   `bson:"command"`
	Includes Includes `bson:"includes,omitempty"`
	IsDrop   bool     `bson:"drop,omitempty"`
	License  string   `bson:"license,omitempty"`
	Port     int      `bson:"port,omitempty"`
	Source   string   `bson:"source"`
	Spool    string   `bson:"spool,omitempty"`
	Target   string   `bson:"target"`
	Verbose  bool     `bson:"verbose,omitempty"`
	Workers  int      `bson:"workers,omitempty"`
	Yes      bool     `bson:"yes,omitempty"`

	genesis     time.Time
	isExit      bool
	included    map[string]*Include
	mutex       sync.Mutex
	replicas    map[string]string
	sourceStats *mdb.ClusterStats
	streamers   []*OplogStreamer
	targetStats *mdb.ClusterStats
	workspace   Workspace
}

var migratorInstance *Migrator
var once sync.Once

// NewMigratorInstance sets and returns a migrator instance
func NewMigratorInstance(filename string) (*Migrator, error) {
	inst, err := ReadMigratorConfig(filename)
	if err != nil {
		return inst, err
	}
	if err = ValidateMigratorConfig(inst); err != nil {
		return inst, err
	}
	if inst.Verbose {
		gox.GetLogger().SetLoggerLevel(gox.Debug)
	}
	inst.genesis = time.Now()
	// establish work space
	os.Mkdir(inst.Spool, 0755)
	inst.workspace = Workspace{dbName: MetaDBName, dbURI: inst.Target, spool: inst.Spool}
	// get clusters stats
	inst.sourceStats = mdb.NewClusterStats("")
	client, err := GetMongoClient(inst.Source)
	if err != nil {
		return migratorInstance, fmt.Errorf("source GetMongoClient failed: %v", err)
	}
	if err = inst.SourceStats().GetClusterStatsSummary(client); err != nil {
		return migratorInstance, err
	}
	inst.targetStats = mdb.NewClusterStats("")
	if client, err = GetMongoClient(inst.Target); err != nil {
		return migratorInstance, fmt.Errorf("target GetMongoClient failed: %v", err)
	}
	if err = inst.TargetStats().GetClusterStatsSummary(client); err != nil {
		return migratorInstance, err
	}
	// create includes map
	inst.included = map[string]*Include{}
	for _, include := range inst.Includes {
		inst.included[include.Namespace] = include
	}
	// set uri map
	inst.replicas = map[string]string{}
	list, err := GetAllReplicas(inst.Source)
	if err != nil {
		return nil, fmt.Errorf("GetAllReplicas failed: %v", err)
	}
	for _, replica := range list {
		cs, err := mdb.ParseURI(replica)
		if err != nil || cs.ReplicaSet == "" {
			return nil, fmt.Errorf("ParseURI failed: %v", err)
		}
		inst.Replicas()[cs.ReplicaSet] = replica
	}
	migratorInstance = inst
	return migratorInstance, nil
}

// GetMigratorInstance returns Migratro migratorInstance
func GetMigratorInstance() *Migrator {
	return migratorInstance
}

// IsExit returns isExit
func (inst *Migrator) IsExit() bool {
	inst.mutex.Lock()
	defer inst.mutex.Unlock()
	return inst.isExit
}

// NotifyWorkerExit set isExit to true
func (inst *Migrator) NotifyWorkerExit() {
	inst.mutex.Lock()
	defer inst.mutex.Unlock()
	inst.isExit = true
}

// AddOplogStreamer returns isExit
func (inst *Migrator) AddOplogStreamer(streamer *OplogStreamer) {
	inst.mutex.Lock()
	defer inst.mutex.Unlock()
	inst.streamers = append(inst.streamers, streamer)
}

// LiveStreamingOplogs set isExit to true
func (inst *Migrator) LiveStreamingOplogs() {
	inst.mutex.Lock()
	defer inst.mutex.Unlock()
	for _, streamer := range inst.streamers {
		streamer.LiveStream()
	}
}

// ResetIncludesTo is a convenient function for go tests
func (inst *Migrator) ResetIncludesTo(includes Includes) {
	migratorInstance.Includes = includes
	migratorInstance.included = map[string]*Include{}
	if includes != nil {
		for _, include := range includes {
			dbName, _ := mdb.SplitNamespace(include.Namespace)
			if include.To != "" {
				dbName, _ = mdb.SplitNamespace(include.To)
			}
			migratorInstance.included[dbName] = include
		}
	}
}

// DropCollections drops all qualified collections
func (inst *Migrator) DropCollections() error {
	logger := gox.GetLogger("DropCollections")
	ctx := context.Background()
	client, err := GetMongoClient(inst.Target)
	if err != nil {
		return err
	}
	if len(inst.Includes) == 0 { // drop all
		dbNames, err := GetQualifiedDBs(client, MetaDBName)
		if err != nil {
			return err
		}
		for _, dbName := range dbNames {
			logger.Debug("drop database " + dbName)
			if err = client.Database(dbName).Drop(ctx); err != nil {
				return err
			}
		}
	} else {
		for _, include := range inst.Included() {
			dbName, collName := mdb.SplitNamespace(include.Namespace)
			if collName == "" || collName == "*" {
				logger.Debug("drop database " + dbName)
				if err = client.Database(dbName).Drop(ctx); err != nil {
					return err
				}
			} else {
				if include.To != "" {
					dbName, collName = mdb.SplitNamespace(include.To)
				}
				logger.Infof("drop namespace %v.%v", dbName, collName)
				if err = client.Database(dbName).Collection(collName).Drop(ctx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// CheckIfBalancersDisabled check if both source and target balancers are disabled
func (inst *Migrator) CheckIfBalancersDisabled() error {
	var err error
	if inst.SourceStats().Cluster == mdb.Sharded {
		if err = checkIfBalancerDisabled(inst.Source); err != nil {
			return fmt.Errorf("source cluster error: %v", err)
		}
	}
	if inst.TargetStats().Cluster == mdb.Sharded {
		if err = checkIfBalancerDisabled(inst.Target); err != nil {
			return fmt.Errorf("target cluster error: %v", err)
		}
	}
	return nil
}

// Workspace returns Workspace
func (inst *Migrator) Workspace() Workspace {
	return inst.workspace
}

// Included returns includes
func (inst *Migrator) Included() map[string]*Include {
	return inst.included
}

// Replicas returns replica URI map
func (inst *Migrator) Replicas() map[string]string {
	return inst.replicas
}

// SourceStats returns stats
func (inst *Migrator) SourceStats() *mdb.ClusterStats {
	return inst.sourceStats
}

// TargetStats returns stats
func (inst *Migrator) TargetStats() *mdb.ClusterStats {
	return inst.targetStats
}

// SkipNamespace skips namespace
func (inst *Migrator) SkipNamespace(namespace string) bool {
	if len(inst.included) == 0 {
		return false
	}
	dbName, collName := mdb.SplitNamespace(namespace)
	allCollsInDB := dbName + ".*"
	if inst.included[allCollsInDB] != nil || inst.included[namespace] != nil {
		return false
	}
	collInAllDB := "*." + collName
	if inst.included[collInAllDB] != nil || inst.included[namespace] != nil {
		return false
	}
	return true
}

// GetToNamespace returns target namespace
func (inst *Migrator) GetToNamespace(ns string) string {
	if len(inst.included) == 0 {
		return ns
	}
	if inst.included[ns] != nil && inst.included[ns].To != "" {
		return inst.included[ns].To
	}
	return ns
}

// ReadMigratorConfig validates configuration from a file
func ReadMigratorConfig(filename string) (*Migrator, error) {
	var err error
	var data []byte
	migrator := Migrator{}
	if data, err = ioutil.ReadFile(filename); err != nil {
		return nil, err
	} else if err = bson.UnmarshalExtJSON(data, false, &migrator); err != nil {
		return nil, err
	}
	return &migrator, ValidateMigratorConfig(&migrator)
}

// ValidateMigratorConfig validates configuration from a file
func ValidateMigratorConfig(migrator *Migrator) error {
	if migrator.Command == "" {
		return fmt.Errorf(`command is required`)
	} else if migrator.Source == "" || migrator.Target == "" {
		return fmt.Errorf(`source and target must have validate connection strings`)
	} else if migrator.Workers > MaxNumberWorkers {
		return fmt.Errorf("number of workers must be between 1 and %v", MaxNumberWorkers)
	} else if migrator.IsDrop && (migrator.Command == CommandData || migrator.Command == CommandDataOnly) {
		return fmt.Errorf(`cannot set {"drop": true} when command is %v`, migrator.Command)
	}
	var logger = gox.GetLogger("ValidateMigratorConfig")
	var values []string
	if migrator.Block <= 0 {
		values = append(values, fmt.Sprintf(`"block":%v`, MaxBlockSize))
		migrator.Block = MaxBlockSize
	}
	if migrator.Port <= 0 {
		values = append(values, fmt.Sprintf(`"port":%v`, Port))
		migrator.Port = Port
	}
	if migrator.Spool == "" {
		values = append(values, fmt.Sprintf(`"spool":"%v"`, DefaultSpool))
		migrator.Spool = DefaultSpool
	}
	if migrator.Workers < 1 {
		values = append(values, fmt.Sprintf(`"workers":%v`, NumberWorkers))
		migrator.Workers = NumberWorkers
	}
	if len(values) > 0 {
		logger.Info("set default {", strings.Join(values, ","), "}")
	}

	return nil
}

// checkIfBalancerDisabled returns balancer state
func checkIfBalancerDisabled(uri string) error {
	var err error
	var client *mongo.Client
	var enabled bool
	if client, err = GetMongoClient(uri); err != nil {
		return err
	}
	if enabled, err = IsBalancerEnabled(client); err != nil {
		return err
	} else if enabled {
		return fmt.Errorf("balancer is enabled")
	}
	return nil
}
