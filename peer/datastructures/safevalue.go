package datastructures

import (
	"sync"
)

type SafeValue[T any] struct {
	Value T
	Lock  sync.Mutex
}

func (sv *SafeValue[T]) Get() T {
	sv.Lock.Lock()
	defer sv.Lock.Unlock()
	return sv.Value
}

func (sv *SafeValue[T]) Set(value T) {
	sv.Lock.Lock()
	defer sv.Lock.Unlock()
	sv.Value = value
}
