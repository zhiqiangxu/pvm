package pvm

import (
	"sync"
	"sync/atomic"
)

type scheduler struct {
	doneMarker       atomic.Bool
	validationIndex  atomic.Int32
	executionIndex   atomic.Int32
	numActiveTasks   atomic.Int32
	decreaseCount    atomic.Int32
	allTxnStatus     []*txnStatus
	allTxnDependency []*txnDependency // blocked transactions  on each index
	allTxnBlockers   []*txnBlockers   // blocking transactions for each index
	blockSize        int
}

type txnStatus struct {
	sync.RWMutex
	status      uint
	incarnation int
}

type txnBlockers struct {
	sync.RWMutex
	blockers map[int]struct{}
}
type txnDependency struct {
	sync.RWMutex
	dependencies map[int]struct{}
}

const (
	txnStatusReadyToExecute = 0
	txnStatusExecuting      = 1
	txnStatusExecuted       = 2
	txnStatusAborting       = 3
)

var _ Scheduler = (*scheduler)(nil)

func NewScheduler(blockSize int) Scheduler {
	allTxnStatus := make([]*txnStatus, blockSize)
	allTxnDependency := make([]*txnDependency, blockSize)
	allTxnBlockers := make([]*txnBlockers, blockSize)
	for i := 0; i < blockSize; i++ {
		allTxnStatus[i] = &txnStatus{}
		allTxnDependency[i] = &txnDependency{}
		allTxnBlockers[i] = &txnBlockers{}
	}

	return &scheduler{blockSize: blockSize, allTxnStatus: allTxnStatus, allTxnDependency: allTxnDependency, allTxnBlockers: allTxnBlockers}
}

func (s *scheduler) Done() bool {
	return s.doneMarker.Load()
}
func (s *scheduler) NextTask() *Task {
	if s.validationIndex.Load() < s.executionIndex.Load() {
		versionToValidate := s.nextVersionToValidate()
		if versionToValidate != nil {
			return &Task{Version: *versionToValidate, Kind: TaskKindV}
		}
	} else {
		versionToExecute := s.nextVersionToExecute()
		if versionToExecute != nil {
			return &Task{Version: *versionToExecute, Kind: TaskKindE}
		}
	}
	return nil
}
func (s *scheduler) AddDependency(index, blockingIndex int) bool {

	txnDependency := s.allTxnDependency[blockingIndex]
	txnDependency.Lock()

	txnStatus := s.allTxnStatus[blockingIndex]
	txnStatus.Lock()
	// dependency resolved
	if txnStatus.status == txnStatusExecuted {
		txnStatus.Unlock()
		txnDependency.Unlock()
		return false
	}
	txnStatus.Unlock()

	s.allTxnStatus[index].Lock()
	s.allTxnStatus[index].status = txnStatusAborting
	s.allTxnStatus[index].Unlock()

	if txnDependency.dependencies == nil {
		txnDependency.dependencies = make(map[int]struct{})
	}
	txnDependency.dependencies[index] = struct{}{}

	txnDependency.Unlock()

	txnBlockers := s.allTxnBlockers[index]
	txnBlockers.Lock()
	if txnBlockers.blockers == nil {
		txnBlockers.blockers = make(map[int]struct{})
	}
	txnBlockers.blockers[blockingIndex] = struct{}{}
	txnBlockers.Unlock()

	// execution task aborted due to a dependency
	s.numActiveTasks.Add(-1)
	return false
}
func (s *scheduler) FinishExecution(version Version, wroteNewLocation bool) *Task {

	txnStatus := s.allTxnStatus[version.Index]
	txnStatus.Lock()
	if txnStatus.status != txnStatusExecuting {
		panic("status must have been EXECUTING")
	}
	txnStatus.status = txnStatusExecuted
	txnStatus.Unlock()

	txnDependency := s.allTxnDependency[version.Index]
	txnDependency.Lock()
	dependencies := txnDependency.dependencies
	txnDependency.dependencies = nil
	txnDependency.Unlock()

	s.resumeDependencies(version.Index, dependencies)

	if s.validationIndex.Load() > int32(version.Index) { // otherwise index already small enough
		if wroteNewLocation {
			// schedule validation for txn_idx and higher txns
			s.decreaseValidationIndex(version.Index)
		} else {
			return &Task{Version: version, Kind: TaskKindV}
		}
	}

	s.numActiveTasks.Add(-1)
	return nil
}

func (s *scheduler) FinishValidation(txnIndex int, aborted bool) *Task {
	if aborted {
		s.setReadyStatus(txnIndex)
		// schedule validation for higher transactions
		s.decreaseValidationIndex(txnIndex + 1)

		if s.executionIndex.Load() > int32(txnIndex) {
			newVersion := s.tryIncarnation(txnIndex)
			if newVersion != nil {
				// return re-execution task to the caller
				return &Task{Version: *newVersion, Kind: TaskKindE}
			}
		}
	}
	// done with validation task
	s.numActiveTasks.Add(-1)
	// no task returned to the caller
	return nil
}
func (s *scheduler) TryValidationAbort(version Version) bool {
	txnStatus := s.allTxnStatus[version.Index]
	txnStatus.Lock()
	defer txnStatus.Unlock()
	if txnStatus.incarnation == version.Incarnation && txnStatus.status == txnStatusExecuted {
		txnStatus.status = txnStatusAborting
		return true
	}

	return false
}

func (s *scheduler) resumeDependencies(blockingIndex int, dependencies map[int]struct{}) {
	if len(dependencies) == 0 {
		return
	}
	minDepTxnIndex := -1
	for depTxnIndex := range dependencies {
		txnBlockers := s.allTxnBlockers[depTxnIndex]
		txnBlockers.Lock()
		delete(txnBlockers.blockers, blockingIndex)
		canResume := len(txnBlockers.blockers) == 0
		txnBlockers.Unlock()
		if canResume {
			s.setReadyStatus(depTxnIndex)
			if minDepTxnIndex == -1 || depTxnIndex < minDepTxnIndex {
				minDepTxnIndex = depTxnIndex
			}
		}

	}

	if minDepTxnIndex != -1 {
		// ensure dependent indices get re-executed
		s.decreaseExecutionIndex(minDepTxnIndex)
	}
}

func (s *scheduler) setReadyStatus(txnIndex int) {
	txnStatus := s.allTxnStatus[txnIndex]
	txnStatus.Lock()
	txnStatus.incarnation += 1
	txnStatus.status = txnStatusReadyToExecute
	txnStatus.Unlock()
}

func (s *scheduler) nextVersionToValidate() *Version {
	if s.validationIndex.Load() >= int32(s.blockSize) {
		s.checkDone()
		return nil
	}

	s.numActiveTasks.Add(1)
	validationIndex := s.validationIndex.Add(1) - 1
	if validationIndex < int32(s.blockSize) {
		txnStatus := s.allTxnStatus[validationIndex]
		txnStatus.RLock()
		status, incarnation := txnStatus.status, txnStatus.incarnation
		txnStatus.RUnlock()
		if status == txnStatusExecuted {
			return &Version{Index: int(validationIndex), Incarnation: incarnation}
		}
	}

	s.numActiveTasks.Add(-1)
	return nil
}

func (s *scheduler) nextVersionToExecute() *Version {
	if s.executionIndex.Load() >= int32(s.blockSize) {
		s.checkDone()
		return nil
	}

	s.numActiveTasks.Add(1)
	executionIndex := int(s.executionIndex.Add(1) - 1)

	return s.tryIncarnation(executionIndex)
}

func (s *scheduler) tryIncarnation(txnIndex int) *Version {
	if txnIndex < s.blockSize {
		txnStatus := s.allTxnStatus[txnIndex]
		txnStatus.Lock()
		if txnStatus.status == txnStatusReadyToExecute {
			txnStatus.status = txnStatusExecuting
			txnStatus.Unlock()
			return &Version{Index: txnIndex, Incarnation: txnStatus.incarnation}
		}
		txnStatus.Unlock()
	}

	s.numActiveTasks.Add(-1)
	return nil
}

func (s *scheduler) checkDone() {
	observedCount := s.decreaseCount.Load()
	if s.executionIndex.Load() >= int32(s.blockSize) && s.validationIndex.Load() >= int32(s.blockSize) && s.numActiveTasks.Load() == 0 && observedCount == s.decreaseCount.Load() {
		s.doneMarker.Store(true)
	}
}

func (s *scheduler) decreaseExecutionIndex(txnIndexInt int) {
	txnIndex := int32(txnIndexInt)
RETRY:
	executionIndex := s.executionIndex.Load()
	if executionIndex > txnIndex {
		if !s.executionIndex.CompareAndSwap(executionIndex, txnIndex) {
			goto RETRY
		}
	}
	s.decreaseCount.Add(1)
}

func (s *scheduler) decreaseValidationIndex(txnIndexInt int) {
	txnIndex := int32(txnIndexInt)
RETRY:
	validationIndex := s.validationIndex.Load()
	if validationIndex > txnIndex {
		if !s.executionIndex.CompareAndSwap(validationIndex, txnIndex) {
			goto RETRY
		}
	}
	s.decreaseCount.Add(1)
}
