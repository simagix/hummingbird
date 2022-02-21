// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/simagix/gox"
	"go.mongodb.org/mongo-driver/bson"
)

// Migrator stores migration configurations
type Migrator struct {
	Block     int      `bson:"block,omitempty"`
	Command   string   `bson:"command"`
	Includes  Includes `bson:"includes,omitempty"`
	IsDrop    bool     `bson:"drop,omitempty"`
	License   string   `bson:"license,omitempty"`
	Port      int      `bson:"port,omitempty"`
	Source    string   `bson:"source"`
	Target    string   `bson:"target"`
	Verbose   bool     `bson:"verbose,omitempty"`
	Workers   int      `bson:"workers,omitempty"`
	Workspace string   `bson:"workspace,omitempty"`
	Yes       bool     `bson:"yes,omitempty"`
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
	migratorInstance = m
	return migratorInstance, nil
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
		return fmt.Errorf(`cannot set {"drop": true} when command is %v`, &migrator.Command)
	}
	var logger = gox.GetLogger("")
	if migrator.Block <= 0 {
		logger.Infof(`"block" not defined, use default %v`, MaxBlockSize)
		migrator.Block = MaxBlockSize
	}
	if migrator.Port <= 0 {
		logger.Infof(`"port" not defined, use default %v`, Port)
		migrator.Port = Port
	}
	if migrator.Workspace == "" {
		logger.Infof(`"workspace" not defined, use default "%v"`, DefaultWorkspace)
		migrator.Workspace = DefaultWorkspace
	}
	if migrator.Workers < MaxNumberWorkers {
		logger.Infof(`"workers" not defined, use default %v`, MaxNumberWorkers)
		migrator.Workers = MaxNumberWorkers
	}

	return nil
}
