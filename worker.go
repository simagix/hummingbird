// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"fmt"
	"time"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// Worker copies data
func Worker(id string) error {
	inst := GetMigratorInstance()
	var setNames []string
	for setName := range inst.Replicas() {
		setNames = append(setNames, setName)
	}
	logger := gox.GetLogger()
	workerID := fmt.Sprintf("proc %v", id)
	status := fmt.Sprintf(`[%v] joined`, workerID)
	ws := inst.Workspace()
	logger.Info(status)
	ws.Log(status)
	index := 0
	rev := -1
	processed := 0
	printer := message.NewPrinter(language.English)
	btime := time.Now()
	for !inst.IsExit() {
		rev *= -1
		index++
		index := index % len(setNames)
		if time.Since(btime) > 10*time.Minute {
			status := printer.Sprintf("[%v] has processed %d tasks", workerID, processed)
			logger.Info(status)
			btime = time.Now()
		}
		task, err := ws.FindNextTaskAndUpdate(setNames[index], workerID, rev)
		if err != nil {
			if task != nil {
				task.Status = TaskAdded
				ws.UpdateTask(task)
			}
			time.Sleep(10 * time.Second)
			continue
		}
		dbName, collName := mdb.SplitNamespace(task.Namespace)
		dbNameTo, collNameTo := dbName, collName
		if task.Include.To != "" {
			dbNameTo, collNameTo = mdb.SplitNamespace(task.Include.To)
		}
		src, err := GetMongoClient(inst.Replicas()[task.SetName])
		if err != nil {
			task.Status = TaskAdded
			ws.UpdateTask(task)
			time.Sleep(1 * time.Second)
			continue
		}
		tgt, err := GetMongoClient(inst.Target)
		if err != nil {
			task.Status = TaskAdded
			ws.UpdateTask(task)
			time.Sleep(1 * time.Second)
			continue
		}
		if err = task.CopyData(src.Database(dbName).Collection(collName),
			tgt.Database(dbNameTo).Collection(collNameTo)); err != nil {
			task.Status = TaskAdded
		} else {
			task.Status = TaskCompleted
			task.EndTime = time.Now()
		}
		task.UpdatedBy = workerID
		ws.UpdateTask(task)
		processed++
		if (processed)%100 == 1 {
			status := printer.Sprintf("[%v] has processed %d tasks", workerID, processed)
			logger.Info(status)
		}
		time.Sleep(100 * time.Millisecond)
	}
	logger.Infof(`[%v] exits`, workerID)
	return nil
}
