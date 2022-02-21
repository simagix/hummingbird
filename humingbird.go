// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package humingbird

import (
	"flag"
	"fmt"
	"log"

	"github.com/simagix/gox"
)

const (
	// DefaultWorkspace defines default work space
	DefaultWorkspace = "./workspace"
	// MaxBlockSize defines max batch size of a task
	MaxBlockSize = 10000
	// MaxNumberWorkers defines max number of concurrent workers
	MaxNumberWorkers = 8
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

// Run routes to a command
func Run(version string) {
	resume := flag.String("resume", "", "resume a migration from a configuration file")
	start := flag.String("start", "", "start a migration from a configuration file")
	ver := flag.Bool("version", false, "print version info")

	flag.Parse()
	flagset := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { flagset[f.Name] = true })

	if *ver {
		fmt.Println(version)
		return
	}
	logger := gox.GetLogger(version)
	if *start != "" {
		err := Start(*start)
		if err != nil {
			log.Fatal(err)
		}
		return
	} else if *resume != "" {
		err := Resume(*resume)
		if err != nil {
			log.Fatal(err)
		}
		return
	} else {
		logger.Info(version)
	}
}
