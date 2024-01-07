package pvm

import (
	"sync"
	"sync/atomic"
)

type scheduler struct {
	doneMarker      atomic.Bool
	validationIndex atomic.Int32
	executionIndex  atomic.Int32
	numActiveTasks  atomic.Int32
	decreaseCount   atomic.Int32
	allTxnState     []*txnState
	blockSize       int
}

type txnState struct {
	sync.RWMutex
	txnStateInner
}

type txnStateInner struct {
	status       txStatus
	incarnation  int
	blockers     map[int]struct{} // blocking transactions for current transactoin
	dependencies map[int]struct{} // blocked transactions  on current transaction
}

type txStatus uint

const (
	txnStatusReadyToExecute txStatus = iota
	txnStatusExecuting
	txnStatusExecuted
	txnStatusAborting
)

var _ Scheduler = (*scheduler)(nil)

func NewScheduler(blockSize int) Scheduler {
	allTxnStatus := make([]*txnState, blockSize)
	for i := 0; i < blockSize; i++ {
		allTxnStatus[i] = &txnState{}
	}

	return &scheduler{blockSize: blockSize, allTxnState: allTxnStatus}
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

	blockingTxnState := s.allTxnState[blockingIndex]
	blockedTxnState := s.allTxnState[index]

	blockingTxnState.Lock()
	// dependency resolved
	if blockingTxnState.status == txnStatusExecuted {
		blockingTxnState.Unlock()
		return false
	}

	blockedTxnState.Lock()
	blockedTxnState.status = txnStatusAborting

	if blockingTxnState.dependencies == nil {
		blockingTxnState.dependencies = make(map[int]struct{})
	}
	blockingTxnState.dependencies[index] = struct{}{}

	if blockedTxnState.blockers == nil {
		blockedTxnState.blockers = make(map[int]struct{})
	}
	blockedTxnState.blockers[blockingIndex] = struct{}{}

	blockedTxnState.Unlock()
	blockingTxnState.Unlock()

	// execution task aborted due to a dependency
	s.numActiveTasks.Add(-1)
	return true
}
func (s *scheduler) FinishExecution(version Version, wroteNewLocation bool) *Task {

	txnState := s.allTxnState[version.Index]
	txnState.Lock()
	if txnState.status != txnStatusExecuting {
		panic("status must have been EXECUTING")
	}
	txnState.status = txnStatusExecuted

	dependencies := txnState.dependencies
	txnState.dependencies = nil

	s.resumeDependencies(version.Index, dependencies)
	txnState.Unlock()

	if s.validationIndex.Load() > int32(version.Index) { // otherwise index already small enough
		if wroteNewLocation {
			// schedule validation for txn_idx and higher txns
			s.decreaseValidationIndex(version.Index)
		} else {
			return &Task{Version: version, Kind: TaskKindV}
		}
	}

	// done with execution task
	s.numActiveTasks.Add(-1)
	// no task returned to the caller
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
	txnStatus := s.allTxnState[version.Index]
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
		txnState := s.allTxnState[depTxnIndex]

		txnState.Lock()
		delete(txnState.blockers, blockingIndex)
		canResume := len(txnState.blockers) == 0

		if canResume {
			s.setReadyStatusLocked(depTxnIndex)
			txnState.Unlock()
			if minDepTxnIndex == -1 || depTxnIndex < minDepTxnIndex {
				minDepTxnIndex = depTxnIndex
			}
		} else {
			txnState.Unlock()
		}

	}

	if minDepTxnIndex != -1 {
		// ensure dependent indices get re-executed
		s.decreaseExecutionIndex(minDepTxnIndex)
	}
}

func (s *scheduler) setReadyStatus(txnIndex int) {
	txnStatus := s.allTxnState[txnIndex]
	txnStatus.Lock()
	s.setReadyStatusLocked(txnIndex)
	txnStatus.Unlock()
}

func (s *scheduler) setReadyStatusLocked(txnIndex int) {
	txnStatus := s.allTxnState[txnIndex]
	txnStatus.incarnation += 1
	txnStatus.status = txnStatusReadyToExecute
}

func (s *scheduler) nextVersionToValidate() *Version {
	if s.validationIndex.Load() >= int32(s.blockSize) {
		s.checkDone()
		return nil
	}

	s.numActiveTasks.Add(1)
	validationIndex := s.validationIndex.Add(1) - 1
	if validationIndex < int32(s.blockSize) {
		txnStatus := s.allTxnState[validationIndex]
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
		txnStatus := s.allTxnState[txnIndex]
		txnStatus.Lock()
		if txnStatus.status == txnStatusReadyToExecute {
			txnStatus.status = txnStatusExecuting
			version := &Version{Index: txnIndex, Incarnation: txnStatus.incarnation}
			txnStatus.Unlock()
			return version
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
