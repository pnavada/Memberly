package datastructures

import (
	"reflect"
	"sort"
	"sync"

	"peer/types"
)

type SafeList[T any] struct {
	list []T
	lock sync.Mutex
}

func (sl *SafeList[T]) Insert(index int, value T) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	if index >= 0 && index <= len(sl.list) {
		sl.list = append(sl.list[:index], append([]T{value}, sl.list[index:]...)...)
	}
}

func (sl *SafeList[T]) Contains(value T) bool {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for _, v := range sl.list {
		if reflect.DeepEqual(v, value) {
			return true
		}
	}
	return false
}

func (sl *SafeList[T]) Sort(less func(a, b T) bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sort.Slice(sl.list, func(i, j int) bool {
		return less(sl.list[i], sl.list[j])
	})
}

func (sl *SafeList[T]) Replace(newList []T) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sl.list = newList
}

func (sl *SafeList[T]) Add(value T) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	sl.list = append(sl.list, value)
}

func (sl *SafeList[T]) Remove(index int) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	if index >= 0 && index < len(sl.list) {
		sl.list = append(sl.list[:index], sl.list[index+1:]...)
	}
}

func (sl *SafeList[T]) RemoveByValue(value T) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for i, v := range sl.list {
		if reflect.DeepEqual(v, value) {
			sl.list = append(sl.list[:i], sl.list[i+1:]...)
			return
		}
	}
}

func (sl *SafeList[T]) Get(index int) (T, bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	if index >= 0 && index < len(sl.list) {
		return sl.list[index], true
	}
	var zero T
	return zero, false
}

func (sl *SafeList[T]) GetAll() []T {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	return append([]T(nil), sl.list...)
}

func (sl *SafeList[T]) GetByViewAndRequestId(viewId, requestId int) (T, bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for _, item := range sl.list {
		if rm, ok := any(item).(types.RequestMessage); ok {
			if rm.ViewId == viewId && rm.RequestId == requestId {
				return item, true
			}
		}
	}
	var zero T
	return zero, false
}

func (sl *SafeList[T]) GetByPeerIdAndOperation(peerId int, operation types.OperationType) (T, bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for _, item := range sl.list {
		if rm, ok := any(item).(types.RequestMessage); ok {
			if rm.PeerId == peerId && rm.Operation == operation {
				return item, true
			}
		}
	}
	var zero T
	return zero, false
}

func (sl *SafeList[T]) RemoveByViewAndRequestId(viewId, requestId int) {
	sl.lock.Lock()
	defer sl.lock.Unlock()
	for i, item := range sl.list {
		if rm, ok := any(item).(types.RequestMessage); ok {
			if rm.ViewId == viewId && rm.RequestId == requestId {
				sl.list = append(sl.list[:i], sl.list[i+1:]...)
				return
			}
		}
	}
}
