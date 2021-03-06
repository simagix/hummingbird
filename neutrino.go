// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/simagix/gox"
)

const (
	// DefaultSpool defines default work space
	DefaultSpool = "./spool"
	// MaxBlockSize defines max batch size of a task
	MaxBlockSize = 10000
	// MaxNumberWorkers defines max number of concurrent workers
	MaxNumberWorkers = 16
	// NumberWorkers defines max number of concurrent workers
	NumberWorkers = 8
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

var fullVersion = "simagix/neutrino"

// Neutrino routes to a command
func Neutrino(version string) error {
	fullVersion = version
	compare := flag.String("compare", "", "deep two clusters")
	resume := flag.String("resume", "", "resume a migration from a configuration file")
	sim := flag.String("sim", "", "simulate data gen")
	start := flag.String("start", "", "start a migration from a configuration file")
	ver := flag.Bool("version", false, "print version info")
	worker := flag.String("worker", "", "start a neutrino worker")

	flag.Parse()
	flagset := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { flagset[f.Name] = true })

	if *ver {
		fmt.Println(version)
		return nil
	}
	logger := gox.GetLogger(version, false) // print version and disable in-mem logs
	if *compare != "" {
		return Compare(*compare)
	} else if *resume != "" {
		return Resume(*resume)
	} else if *sim != "" {
		return Simulate(*sim)
	} else if *start != "" {
		return Start(*start)
	} else if *worker != "" {
		inst, err := NewMigratorInstance(*worker)
		if err != nil {
			return fmt.Errorf("NewMigratorInstance failed: %v", err)
		}
		wg := gox.NewWaitGroup(inst.Workers)
		for i := 0; i < inst.Workers; i++ { // start all workers
			procID := fmt.Sprintf("%v.%v", os.Getpid(), i+1)
			wg.Add(1)
			go Worker(procID)
			time.Sleep(10 * time.Millisecond)
		}
		wg.Wait()
		return nil
	}
	logger.Info(version)
	return nil
}
