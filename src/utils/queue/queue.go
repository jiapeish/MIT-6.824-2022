package queue

import (
	"sync/atomic"
	"unsafe"
)

// LockFreeQueue implements a queue without lock
type LockFreeQueue struct {
	Head unsafe.Pointer
	Tail unsafe.Pointer
	Size int32
}

type Node struct {
	Val  interface{}
	Next unsafe.Pointer
}

// NewLockFreeQueue creates an queue
func NewLockFreeQueue() *LockFreeQueue {
	node := &Node{}
	return &LockFreeQueue{
		Head: node,
		Tail: node,
		Size: 0,
	}
}

// Enqueue push value v to the tail of the queue.
func (lfq *LockFreeQueue) Enqueue(v interface{}) {
	node := &Node{Val: v}
	for {
		tail := (*Node)(atomic.LoadPointer(&lfq.Tail))
		next := (*Node)(atomic.LoadPointer(&tail.Next))
		if tail == (*Node)(atomic.LoadPointer(&lfq.Tail)) { // Are tail and next consistent?
			// Was Tail pointing to the last node?
			if next == nil {
				// Try to link node at the end of the linked list
				if atomic.CompareAndSwapPointer(&tail.Next, next, node) {
					atomic.CompareAndSwapPointer(&lfq.Tail, tail, node) // Enqueue is done.  Try to swing Tail to the inserted node
					atomic.AddInt32(&lfq.Size, 1)
					return // Enqueue is done.  Exit loop
				}
			} else { // Tail was not pointing to the last node
				// Try to swing Tail to the next node
				atomic.CompareAndSwapPointer(&lfq.Tail, tail, next)
			}
		}
	}
}

// Dequeue pop value from the head of the queue.
func (lfq *LockFreeQueue) Dequeue() interface{} {
	for {
		head := (*Node)(atomic.LoadPointer(&lfq.Head))
		tail := (*Node)(atomic.LoadPointer(&lfq.Tail))
		next := (*Node)(atomic.LoadPointer(&head.Next))
		if head == (*Node)(atomic.LoadPointer(&lfq.Head)) { // Are head, tail, and next consistent?
			if head == tail { // Is queue empty or Tail falling behind?
				if next == nil { // Is queue empty?
					return nil
				}
				// Tail is falling behind.  Try to advance it
				atomic.CompareAndSwapPointer(&lfq.Tail, tail, next)
			} else { // No need to deal with Tail
				// Read value before CAS
				// Otherwise, another dequeue might free the next node
				v := next.Val
				// Try to swing Head to the next node
				if atomic.CompareAndSwapPointer(&lfq.Head, head, next) {
					// Queue was not empty, dequeue succeeded
					atomic.AddInt32(&lfq.Size, -1)
					return v // Dequeue is done.  Exit loop
				}
			}
		}
	}
}
