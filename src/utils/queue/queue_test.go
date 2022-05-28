package queue

import "testing"

func TestLockFreeQueue(t *testing.T) {
	loops := 1000 * 1000
	lfq := NewLockFreeQueue()
	t.Run("test lock free queue", func(t *testing.T) {
		for i := 0; i < loops; i++ {
			lfq.Enqueue(i)
		}

		for i := 0; i < loops; i++ {
			val := lfq.Dequeue()
			if val == nil {
				t.Fatalf("[LockFreeQueue] failed, get null element in the queue")
			}
			if val.(int) != i {
				t.Fatalf("[LockFreeQueue] failed, get wrong element(%v) in the queue, should be (%d)", val, i)
			}
		}
	})
}
