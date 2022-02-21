// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"fmt"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/simagix/keyhole/mdb"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
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
	Target   string   `bson:"target"`
	Verbose  bool     `bson:"verbose,omitempty"`
	Workers  int      `bson:"workers,omitempty"`
	Staging  string   `bson:"staging,omitempty"`
	Yes      bool     `bson:"yes,omitempty"`

	sourceStats *mdb.ClusterStats
	targetStats *mdb.ClusterStats
	workspace   Workspace
}

var migratorInstance *Migrator
var once sync.Once

// NewMigratorInstance sets and returns a migrator instance
func NewMigratorInstance(filename string) (*Migrator, error) {
	m, err := ReadMigratorConfig(filename)
	if err != nil {
		return m, err
	}
	if err = ValidateMigratorConfig(m); err != nil {
		return m, err
	}
	// establish work space
	m.workspace = Workspace{dbName: MetaDBName, dbURI: m.Target, staging: m.Staging}
	// get clusters stats
	m.sourceStats = mdb.NewClusterStats("")
	client, err := GetMongoClient(m.Source)
	if err = m.sourceStats.GetClusterStatsSummary(client); err != nil {
		return migratorInstance, err
	}
	m.targetStats = mdb.NewClusterStats("")
	client, err = GetMongoClient(m.Target)
	if err = m.targetStats.GetClusterStatsSummary(client); err != nil {
		return migratorInstance, err
	}
	migratorInstance = m
	return migratorInstance, nil
}

// SetMigratorInstance changes the instance, for go tests
func SetMigratorInstance(migrator *Migrator) {
	migratorInstance = migrator
}

// GetMigratorInstance returns Migratro migratorInstance
func GetMigratorInstance() *Migrator {
	return migratorInstance
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
	var logger = gox.GetLogger("")
	var values []string
	if migrator.Block <= 0 {
		values = append(values, fmt.Sprintf(`"block":%v`, MaxBlockSize))
		migrator.Block = MaxBlockSize
	}
	if migrator.Port <= 0 {
		values = append(values, fmt.Sprintf(`"port":%v`, Port))
		migrator.Port = Port
	}
	if migrator.Staging == "" {
		values = append(values, fmt.Sprintf(`"workspace":"%v"`, DefaultStaging))
		migrator.Staging = DefaultStaging
	}
	if migrator.Workers < MaxNumberWorkers {
		values = append(values, fmt.Sprintf(`"workers":%v`, MaxNumberWorkers))
		migrator.Workers = MaxNumberWorkers
	}
	if len(values) > 0 {
		logger.Info("set default {", strings.Join(values, ","), "}")
	}

	return nil
}
