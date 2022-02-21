// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"fmt"

	"github.com/simagix/gox"
)

// Start starts a migration
func Start(filename string) error {
	gox.GetLogger("").Remarkf("start a migration from %v", filename)
	var err error
	var isConfig, isData, isOplog bool
	m, err := NewMigratorInstance(filename)
	if err != nil {
		return err
	}
	if m.Command == CommandIndex {
		return IndexCopier()
	} else if m.Command == CommandAll {
		isConfig = true
		isData = true
		isOplog = true
	} else if m.Command == CommandConfig {
		isConfig = true
	} else if m.Command == CommandData {
		if m.IsDrop {
			return fmt.Errorf(`cannot use {"drop": true} with {"command": "data"}`)
		}
		isData = true
		isOplog = true
	} else if m.Command == CommandDataOnly {
		if m.IsDrop {
			return fmt.Errorf(`cannot use {"drop": true} with {"command": "data-only"}`)
		}
		isData = true
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
		if err = DataCopier(); err != nil {
			return err
		}
	}
	return nil
}
