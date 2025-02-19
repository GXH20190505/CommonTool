package inibase

/** 说明
"github.com/nsqio/nsq/internal/util"
根据nsq中的退出方案设计
主要功能：
1、对子协程进行处理，可以优雅的退出协程
*/

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
