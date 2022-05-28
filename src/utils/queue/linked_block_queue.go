package queue

import (
	"container/list"
	"sync"
)

type LinkedBlockQueue struct {
	list *list.List
	cond *sync.Cond
}

func NewLinkedBlockQueue() *LinkedBlockQueue {
	return &LinkedBlockQueue{
		list: list.New(),
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (lbq *LinkedBlockQueue) PushFront(data interface{}) {
	lbq.cond.L.Lock()
	lbq.list.PushFront(data)
	lbq.cond.L.Unlock()
	lbq.cond.Broadcast()
}

func (lbq *LinkedBlockQueue) PushBack(data interface{}) {
	lbq.cond.L.Lock()
	lbq.list.PushBack(data)
	lbq.cond.L.Unlock()
	lbq.cond.Broadcast()
}

func (lbq *LinkedBlockQueue) PeekFront() interface{} {
	var value interface{}

	lbq.cond.L.Lock()
	for lbq.list.Len() == 0 {
		lbq.cond.Wait()
	}
	element := lbq.list.Front()
	if element != nil {
		value = element.Value
	}
	lbq.cond.L.Unlock()

	return value
}

func (lbq *LinkedBlockQueue) PeekBack() interface{} {
	var value interface{}

	lbq.cond.L.Lock()
	for lbq.list.Len() == 0 {
		lbq.cond.Wait()
	}
	element := lbq.list.Back()
	if element != nil {
		value = element.Value
	}
	lbq.cond.L.Unlock()

	return value
}

func (lbq *LinkedBlockQueue) PopFront() interface{} {
	var value interface{}

	lbq.cond.L.Lock()
	element := lbq.list.Front()
	if element != nil {
		lbq.list.Remove(element)
		value = element.Value
	}
	lbq.cond.L.Unlock()

	return value
}

func (lbq *LinkedBlockQueue) PopBack() interface{} {
	var value interface{}

	lbq.cond.L.Lock()
	element := lbq.list.Back()
	if element != nil {
		lbq.list.Remove(element)
		value = element.Value
	}
	lbq.cond.L.Unlock()

	return value
}

func (lbq *LinkedBlockQueue) BlockPopFront() interface{} {
	var value interface{}

	lbq.cond.L.Lock()
	for lbq.list.Len() == 0 {
		lbq.cond.Wait()
	}
	element := lbq.list.Front()
	if element != nil {
		lbq.list.Remove(element)
		value = element.Value
	}
	lbq.cond.L.Unlock()

	return value
}

func (lbq *LinkedBlockQueue) BlockPopBack() interface{} {
	var value interface{}

	lbq.cond.L.Lock()
	for lbq.list.Len() == 0 {
		lbq.cond.Wait()
	}
	element := lbq.list.Back()
	if element != nil {
		lbq.list.Remove(element)
		value = element.Value
	}
	lbq.cond.L.Unlock()

	return value
}

func (lbq *LinkedBlockQueue) Len() int {
	lbq.cond.L.Lock()
	size := lbq.list.Len()
	lbq.cond.L.Unlock()
	return size
}
