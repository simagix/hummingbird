// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"fmt"

	"github.com/simagix/gox"
)

// Start starts a migration
func Start(filename string) error {
	gox.GetLogger("Start").Remarkf("start a migration from %v", filename)
	var err error
	var isConfig, isData, isOplog bool
	inst, err := NewMigratorInstance(filename)
	if err != nil {
		return err
	}
	if inst.Command == CommandIndex {
		return IndexCopier()
	} else if inst.Command == CommandAll {
		isConfig = true
		isData = true
		isOplog = true
	} else if inst.Command == CommandConfig {
		isConfig = true
	} else if inst.Command == CommandData || inst.Command == CommandDataOnly {
		if inst.IsDrop {
			return fmt.Errorf(`cannot use {"drop": true} with {"command": "%v"}`, inst.Command)
		}
		isData = true
		if inst.Command == CommandData {
			isOplog = true
		}
	}

	if isData {
		if err = inst.CheckIfBalancersDisabled(); err != nil { // if balancer is running, exits
			return err
		}
	}

	if inst.IsDrop {
		if err = inst.DropCollections(); err != nil {
			return err
		}
	}

	if isOplog {
		OplogCopier()
	}

	if isConfig {
		if err = ConfigCopier(); err != nil {
			return err
		}
	}

	if isData {
		GetMigratorInstance().workspace.Reset() // reset meta data and clean up staging
		if err = DataCopier(); err != nil {
			return err
		}
	}
	return nil
}
