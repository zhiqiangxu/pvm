package main

import (
	"fmt"
	"time"

	"github.com/zhiqiangxu/pvm"
)

type vm struct {
}

func NewVM() *vm {
	return &vm{}
}

var _ pvm.VM[int] = (*vm)(nil)

func (vm *vm) Execute(txnIndex int) (result pvm.VMResult[int]) {
	return
}

func main() {

	start := time.Now()
	pvm.NewExecutor[int](5, NewVM(), 100 /*blockSize*/).Run()
	fmt.Println("execution took", time.Since(start))
}
