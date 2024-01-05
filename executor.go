package pvm

import (
	"sync"

	"github.com/zhiqiangxu/util"
)

type executor[L comparable] struct {
	concurrency int
	vm          VM[L]
	scheduler   Scheduler
	mvmemory    MVMemory[L]
}

var _ Executor = (*executor[int])(nil)

func NewExecutor[L comparable](concurrency int, vm VM[L], blockSize int) Executor {
	return &executor[L]{
		concurrency: concurrency,
		vm:          vm,
		scheduler:   NewScheduler(blockSize),
		mvmemory:    NewMVMemory[L](blockSize)}
}

func NewExecutorWithDeps[L comparable](concurrency int, vm VM[L], blockSize int, allDeps [][]int) Executor {
	scheduler := NewScheduler(blockSize)

	for index, deps := range allDeps {
		for _, depIndex := range deps {
			scheduler.AddDependency(index, depIndex)
		}
	}
	return &executor[L]{
		concurrency: concurrency,
		vm:          vm,
		scheduler:   scheduler,
		mvmemory:    NewMVMemory[L](blockSize)}
}

func (e *executor[L]) Run() {
	var wg sync.WaitGroup
	for i := 0; i < e.concurrency; i++ {
		util.GoFunc(&wg, e.run)
	}
	wg.Wait()
}

func (e *executor[L]) run() {

	var task *Task
	for {
		if task != nil {
			switch task.Kind {
			case TaskKindE:
				task = e.tryExecute(task.Version)
			case TaskKindV:
				task = e.tryValidate(task.Version)
			default:
				panic("invalid task kind")
			}

		}
		if task == nil {
			task = e.scheduler.NextTask()
		}

		if task == nil && e.scheduler.Done() {
			break
		}
	}
}

func (e *executor[L]) tryExecute(version Version) (task *Task) {
	vmResult := e.vm.Execute(version.Index)
	if vmResult.Status == VMStatusReadError {
		if !e.scheduler.AddDependency(version.Index, vmResult.BlockingIndex) {
			task = e.tryExecute(version)
		}
	} else {
		wroteNewLocation := e.mvmemory.Record(version, vmResult.ReadSet, vmResult.WriteSet)
		task = e.scheduler.FinishExecution(version, wroteNewLocation)
	}
	return
}

func (e *executor[L]) tryValidate(version Version) (task *Task) {
	readSetValid := e.mvmemory.ValidateReadSet(version.Index)
	aborted := !readSetValid && e.scheduler.TryValidationAbort(version)
	if aborted {
		e.mvmemory.ConvertWritesToEstimates(version.Index)
	}
	return e.scheduler.FinishValidation(version.Index, aborted)
}
