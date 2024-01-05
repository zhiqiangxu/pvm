package pvm

type Version struct {
	// transaction index in block
	Index       int
	Incarnation int
}

type ReadSet[L comparable] []ReadDescriptor[L]

type ReadDescriptor[L comparable] struct {
	Location L
	V        *Version
}

type WriteDescriptor[L comparable] struct {
	Location L
	Val      interface{}
}

type WriteSet[L comparable] []WriteDescriptor[L]

type Executor interface {
	Run()
}

type LocationValue[L comparable] struct {
	Location L
	Value    interface{}
}

type MVMemory[L comparable] interface {
	Record(Version, ReadSet[L], WriteSet[L]) bool
	Read(location L, txnIndex int) ReadResult
	Snapshot() []LocationValue[L]
	ValidateReadSet(txnIndex int) bool
	ConvertWritesToEstimates(txnIndex int)
}

type ReadResult struct {
	Status  ReadStatus
	Version Version
	Value   interface{}
	// blocking transaction index
	BlockingIndex int
}

type ReadStatus int

const (
	ReadStatusOK ReadStatus = iota
	ReadStatusNotFound
	ReadStatusError
)

type VM[L comparable] interface {
	Execute(txnIndex int) VMResult[L]
}

type VMResult[L comparable] struct {
	ReadSet  ReadSet[L]
	WriteSet WriteSet[L]
	Status   VMStatus
	// blocking transaction index
	BlockingIndex int
}

type VMStatus int

const (
	VMStatusOK VMStatus = iota
	VMStatusReadError
)

type TaskKind int

const (
	TaskKindE TaskKind = iota
	TaskKindV
)

type Task struct {
	Kind    TaskKind
	Version Version
}

type Scheduler interface {
	Done() bool
	NextTask() *Task
	AddDependency(index, blockingIndex int) bool
	FinishExecution(version Version, wroteNewLocation bool) *Task
	FinishValidation(txnIndex int, aborted bool) *Task
	TryValidationAbort(Version) bool
}
