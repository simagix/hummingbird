// Copyright Kuei-chun Chen, 2022-present. All rights reserved.

package hummingbird

import (
	"fmt"
	"time"

	"github.com/simagix/gox"
	"github.com/simagix/keyhole/mdb"
)

// Worker copies data
func Worker(id int) error {
	inst := GetMigratorInstance()
	var setNames []string
	for setName := range inst.Replicas() {
		setNames = append(setNames, setName)
	}
	logger := gox.GetLogger("Worker")
	workerID := fmt.Sprintf("worker-%v", id)
	logger.Infof(`[%v] joined`, workerID)
	ws := inst.Workspace()
	index := 0
	rev := -1
	for !inst.IsExit() {
		rev *= -1
		index++
		index := index % len(setNames)
		task, err := ws.FindNextTaskAndUpdate(setNames[index], workerID, rev)
		if err != nil {
			if task != nil {
				task.Status = TaskAdded
				ws.UpdateTask(task)
			}
			time.Sleep(1 * time.Second)
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
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}
