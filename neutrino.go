// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"flag"
	"fmt"

	"github.com/simagix/gox"
)

const (
	// DefaultStaging defines default work space
	DefaultStaging = "./workspace"
	// MaxBlockSize defines max batch size of a task
	MaxBlockSize = 10000
	// MaxNumberWorkers defines max number of concurrent workers
	MaxNumberWorkers = 16
	// Port defines port number to listen to
	Port = 3629
)

const (
	// CommandAll copies all
	CommandAll = "all"
	// CommandConfig copies configurations
	CommandConfig = "config"
	// CommandData copies data and tail oplogs after completion
	CommandData = "data"
	// CommandDataOnly copies data only
	CommandDataOnly = "data-only"
	// CommandIndex copies indexes
	CommandIndex = "index"
	// CommandOplog tails oplogs
	CommandOplog = "oplog"
)

// Neutrino routes to a command
func Neutrino(version string) error {
	resume := flag.String("resume", "", "resume a migration from a configuration file")
	start := flag.String("start", "", "start a migration from a configuration file")
	ver := flag.Bool("version", false, "print version info")

	flag.Parse()
	flagset := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { flagset[f.Name] = true })

	if *ver {
		fmt.Println(version)
		return nil
	}
	logger := gox.GetLogger(version)
	if *start != "" {
		return Start(*start)
	} else if *resume != "" {
		return Resume(*resume)
	}
	logger.Info(version)
	return nil
}
