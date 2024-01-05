# pvm, parallel virtual machine

## Introduction

An implementation of [block-stm](https://arxiv.org/pdf/2203.06871.pdf) with code reuse in mind.

## Usage

Follow the example [here](https://github.com/zhiqiangxu/pvm/blob/main/example/hello/main.go), basically users only need to provide an implementation for [VM](https://github.com/zhiqiangxu/pvm/blob/ba8ddc023b4fd7d9b30618ca5312743545f93c6d/type.go#L56) interface.