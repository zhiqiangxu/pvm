package pvm

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/emirpasic/gods/maps/treemap"
)

type mvMemory[L comparable] struct {
	data         sync.Map
	lastWriteSet []atomic.Pointer[[]L]
	lastReadSet  []atomic.Pointer[ReadSet[L]]
}

type dataCells struct {
	sync.RWMutex
	tm *treemap.Map
}

type dataCell struct {
	flag        flag
	incarnation int
	value       interface{}
}

type flag uint

const (
	flagDone flag = iota
	flagEstimate
)

var _ MVMemory[int] = (*mvMemory[int])(nil)

func NewMVMemory[L comparable](blockSize int) MVMemory[L] {
	return &mvMemory[L]{
		lastWriteSet: make([]atomic.Pointer[[]L], blockSize),
		lastReadSet:  make([]atomic.Pointer[ReadSet[L]], blockSize)}
}

func (mvm *mvMemory[L]) Record(version Version, rs ReadSet[L], ws WriteSet[L]) (wroteNewLocation bool) {

	wroteNewLocation = mvm.applyWriteSet(version, ws)

	mvm.lastReadSet[version.Index].Store(&rs)

	return
}

func (mvm *mvMemory[L]) Read(location L, txnIndex int) (result ReadResult) {
	cells := mvm.getLocationCells(location, func() *dataCells { return nil })
	if cells == nil {
		result.Status = ReadStatusNotFound
		return
	}

	cells.RLock()

	fk, fv := cells.tm.Floor(txnIndex - 1)

	if fk != nil && fv != nil {
		c := fv.(*dataCell)
		switch c.flag {
		case flagEstimate:
			result.Status = ReadStatusError
			result.BlockingIndex = fk.(int)
		case flagDone:
			result.Status = ReadStatusOK
			result.Version = Version{Index: fk.(int), Incarnation: c.incarnation}
			result.Value = c.value
		default:
			panic("should not happen - unknown flag value")
		}

		cells.RUnlock()
	} else {
		cells.RUnlock()
		result.Status = ReadStatusNotFound
	}

	return

}

func (mvm *mvMemory[L]) Snapshot() (ret []LocationValue[L]) {

	mvm.data.Range(func(location, _ any) bool {
		result := mvm.Read(location.(L), len(mvm.lastReadSet))
		if result.Status == ReadStatusOK {
			ret = append(ret, LocationValue[L]{Location: location.(L), Value: result.Value})
		}
		return true
	})
	return
}

func (mvm *mvMemory[L]) ValidateReadSet(txnIndex int) bool /*valid*/ {
	prevReads := mvm.lastReadSet[txnIndex].Load()
	if prevReads != nil {
		for _, read := range *prevReads {
			curRead := mvm.Read(read.Location, txnIndex)

			if curRead.Status == ReadStatusError {
				return false
			}
			if curRead.Status == ReadStatusNotFound && read.V != nil {
				return false
			}
			if curRead.Status == ReadStatusOK && (read.V == nil || curRead.Version != *read.V) {
				return false
			}
		}
	}
	return true
}

func (mvm *mvMemory[L]) ConvertWritesToEstimates(txnIndex int) {
	prevWrites := mvm.lastWriteSet[txnIndex].Load()
	if prevWrites != nil {
		for _, location := range *prevWrites {
			cells := mvm.getLocationCells(location, func() *dataCells { return nil })
			cells.Lock()

			ci, ok := cells.tm.Get(txnIndex)
			if ok {
				ci.(*dataCell).flag = flagEstimate
			}

			cells.Unlock()
		}
	}
}

func (mvm *mvMemory[L]) applyWriteSet(version Version, ws WriteSet[L]) (wroteNewLocation bool) {
	newLocations := make(map[L]struct{}, len(ws))
	for _, w := range ws {
		mvm.writeData(w.Location, version, w.Val)
		newLocations[w.Location] = struct{}{}
	}

	prevLocations := mvm.lastWriteSet[version.Index].Load()
	if prevLocations != nil {
		prevLocationList := *prevLocations
		prevLocationMap := make(map[L]struct{}, len(prevLocationList))
		for _, location := range prevLocationList {
			prevLocationMap[location] = struct{}{}
		}

		for location := range newLocations {
			if _, ok := prevLocationMap[location]; !ok {
				wroteNewLocation = true
				break
			}
		}

		for location := range prevLocationMap {
			if _, ok := newLocations[location]; !ok {
				mvm.removeData(location, version.Index)
			}
		}
	} else {
		wroteNewLocation = len(newLocations) > 0
	}

	newLocationList := make([]L, 0, len(newLocations))
	for location := range newLocations {
		newLocationList = append(newLocationList, location)
	}
	mvm.lastWriteSet[version.Index].Store(&newLocationList)

	return
}

func (mvm *mvMemory[L]) writeData(location L, version Version, value interface{}) {
	cells := mvm.getLocationCells(location, func() (cells *dataCells) {
		n := &dataCells{
			tm: treemap.NewWithIntComparator(),
		}
		val, _ := mvm.data.LoadOrStore(location, n)
		cells = val.(*dataCells)
		return
	})

	cells.Lock()

	if ci, ok := cells.tm.Get(version.Index); !ok {
		cells.tm.Put(version.Index, &dataCell{
			flag:        flagDone,
			incarnation: version.Incarnation,
			value:       value,
		})
	} else {
		if ci.(*dataCell).incarnation > version.Incarnation {
			panic(fmt.Errorf("existing transaction value does not have lower incarnation: %v, %v",
				location, version.Index))
		}

		ci.(*dataCell).flag = flagDone
		ci.(*dataCell).incarnation = version.Incarnation
		ci.(*dataCell).value = value
	}
	cells.Unlock()
}

func (mvm *mvMemory[L]) removeData(location L, txnIndex int) {
	cells := mvm.getLocationCells(location, func() (cells *dataCells) {
		return
	})
	if cells == nil {
		return
	}
	cells.Lock()
	cells.tm.Remove(txnIndex)
	cells.Unlock()
}

func (mvm *mvMemory[L]) getLocationCells(location L, fGen func() *dataCells) (cells *dataCells) {
	val, ok := mvm.data.Load(location)

	if !ok {
		cells = fGen()
	} else {
		cells = val.(*dataCells)
	}

	return
}
