// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"fmt"
	"log"

	"github.com/simagix/gox"
)

// Resume resumes a migration
func Resume(filename string, extra ...bool) error {
	var err error
	var isData, isOplog bool
	logger := gox.GetLogger()
	inst, err := NewMigratorInstance(filename)
	if err != nil {
		return fmt.Errorf("NewMigratorInstance failed: %v", err)
	}
	ws := inst.Workspace()
	status := fmt.Sprintf("resume a migration from %v", filename)
	logger.Remark(status)
	if err = ws.Log(status); err != nil {
		return fmt.Errorf("update status failed: %v", err)
	}
	if inst.Command == CommandAll {
		isData = true
		isOplog = true
	} else if inst.Command == CommandData || inst.Command == CommandDataOnly {
		if inst.IsDrop {
			return fmt.Errorf(`cannot use {"drop": true} with {"command": "%v"}`, inst.Command)
		}
		isData = true
		if inst.Command == CommandData {
			isOplog = true
		}
	} else {
		return fmt.Errorf("unsupported command %v", inst.Command)
	}
	wg := gox.NewWaitGroup(4)
	if len(extra) == 0 {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			if err := StartWebServer(port); err != nil {
				log.Fatalf("StartWebServer failed: %v", err)
			}
		}(inst.Port)
	}
	if isData {
		if err = inst.CheckIfBalancersDisabled(); err != nil { // if balancer is running, exits
			return fmt.Errorf("CheckIfBalancersDisabled failed: %v", err)
		}
	}

	tasks, err := ws.FindAllParentTasks()
	if err != nil {
		return fmt.Errorf("FindAllParentTasks failed %v", err)
	}
	for _, task := range tasks {
		if task.Status == TaskSplitting {
			ws.ResetParentTask(*task)
		}
	}
	ws.ResetProcessingTasks()

	if isOplog {
		if err = OplogStreamers(); err != nil {
			return fmt.Errorf("OplogStreamers failed: %v", err)
		}
	}
	if isData {
		if err = DataCopier(); err != nil {
			return fmt.Errorf("DataCopier failed: %v", err)
		}
	}
	inst.NotifyWorkerExit()
	inst.LiveStreamingOplogs()
	wg.Wait()
	return nil
}
