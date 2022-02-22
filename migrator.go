// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"context"
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

	included    map[string]*Include
	sourceStats *mdb.ClusterStats
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
	// establish work space
	inst.workspace = Workspace{dbName: MetaDBName, dbURI: inst.Target, staging: inst.Staging}
	// get clusters stats
	inst.sourceStats = mdb.NewClusterStats("")
	client, err := GetMongoClient(inst.Source)
	if err = inst.sourceStats.GetClusterStatsSummary(client); err != nil {
		return migratorInstance, err
	}
	inst.targetStats = mdb.NewClusterStats("")
	client, err = GetMongoClient(inst.Target)
	if err = inst.targetStats.GetClusterStatsSummary(client); err != nil {
		return migratorInstance, err
	}
	inst.included = map[string]*Include{}
	for _, include := range inst.Includes {
		dbName, _ := mdb.SplitNamespace(include.Namespace)
		if include.To != "" {
			dbName, _ = mdb.SplitNamespace(include.To)
		}
		inst.included[dbName] = include
	}
	migratorInstance = inst
	return migratorInstance, nil
}

// GetMigratorInstance returns Migratro migratorInstance
func GetMigratorInstance() *Migrator {
	return migratorInstance
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
			logger.Info("drop database " + dbName)
			if err = client.Database(dbName).Drop(ctx); err != nil {
				return err
			}
		}
	} else {
		for _, include := range inst.included {
			dbName, collName := mdb.SplitNamespace(include.Namespace)
			if collName == "" || collName == "*" {
				logger.Info("drop database " + dbName)
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
