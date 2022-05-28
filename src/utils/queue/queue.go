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
	node := unsafe.Pointer(&Node{})
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
		tail := Load(&lfq.Tail)
		next := Load(&tail.Next)
		if tail == Load(&lfq.Tail) { // Are tail and next consistent?
			// Was Tail pointing to the last node?
			if next == nil {
				// Try to link node at the end of the linked list
				if CAS(&tail.Next, next, node) {
					CAS(&lfq.Tail, tail, node) // Enqueue is done.  Try to swing Tail to the inserted node
					atomic.AddInt32(&lfq.Size, 1)
					return // Enqueue is done.  Exit loop
				}
			} else { // Tail was not pointing to the last node
				// Try to swing Tail to the next node
				CAS(&lfq.Tail, tail, next)
			}
		}
	}
}

// Dequeue pop value from the head of the queue.
func (lfq *LockFreeQueue) Dequeue() interface{} {
	for {
		head := Load(&lfq.Head)
		tail := Load(&lfq.Tail)
		next := Load(&head.Next)
		if head == Load(&lfq.Head) { // Are head, tail, and next consistent?
			if head == tail { // Is queue empty or Tail falling behind?
				if next == nil { // Is queue empty?
					return nil
				}
				// Tail is falling behind.  Try to advance it
				CAS(&lfq.Tail, tail, next)
			} else { // No need to deal with Tail
				// Read value before CAS
				// Otherwise, another dequeue might free the next node
				v := next.Val
				// Try to swing Head to the next node
				if CAS(&lfq.Head, head, next) {
					// Queue was not empty, dequeue succeeded
					atomic.AddInt32(&lfq.Size, -1)
					return v // Dequeue is done.  Exit loop
				}
			}
		}
	}
}

func Load(p *unsafe.Pointer) *Node {
	return (*Node)(atomic.LoadPointer(p))
}

func CAS(p *unsafe.Pointer, old, new *Node) (ok bool) {
	return atomic.CompareAndSwapPointer(p, unsafe.Pointer(old), unsafe.Pointer(new))
}