package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/utils/queue"
	"6.824/utils/set"
)

const (
	Timeout         = 10 * time.Second
	InvalidWorkerId = -1
	InvalidFileId   = -1
)

type MapTask struct {
	wid   int
	fid   int
	start time.Time
}

type ReduceTask struct {
	wid   int
	fid   int
	start time.Time
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	counter int32

	idleMapTasks    *queue.LinkedBlockQueue
	mMu             sync.Mutex
	mapDone         bool
	runningMapTasks *set.Set
	mts             []MapTask

	idleReduceTasks    *queue.LinkedBlockQueue
	rMu                sync.Mutex
	reduceDone         bool
	runningReduceTasks *set.Set
	rts                []ReduceTask

	prefix string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignMapTask(args *MapArgs, reply *MapReply) error {
	reply.Wid = args.Wid
	if reply.Wid == InvalidWorkerId {
		reply.Wid = int(atomic.LoadInt32(&c.counter)) // simply allocate
		atomic.AddInt32(&c.counter, 1)
	}
	Printf(c.prefix, "worker(%v) requests for a map task\n", reply.Wid)

	c.mMu.Lock()
	if c.mapDone {
		c.mMu.Unlock()
		setFlag(reply)
		return nil
	}

	if c.idleMapTasks.Len() == 0 && c.runningMapTasks.Len() == 0 {
		c.mapDone = true
		c.mMu.Unlock()
		setFlag(reply)
		c.shuffleTasks()
		return nil
	}
	Printf(c.prefix, "map tasks status: idle(%d) running(%d)\n", c.idleMapTasks.Len(), c.runningMapTasks.Len())
	c.mMu.Unlock() // release lock to allow idle update

	now := time.Now()

	fileId := InvalidFileId
	value := c.idleMapTasks.PopFront()
	if value != nil {
		fileId = value.(int)
		c.mMu.Lock()
		reply.Filename = c.files[fileId]
		c.mts[fileId].start = now
		c.mts[fileId].wid = reply.Wid
		c.runningMapTasks.Add(fileId)
		c.mMu.Unlock()

		Printf(c.prefix, "add map task(%d-%s-%s) to running set\n", fileId, reply.Filename, now.String())
	}

	reply.Fid = fileId
	reply.Completed = false
	reply.NReduce = c.nReduce

	return nil
}

func (c *Coordinator) HandleMapTaskStatus(args *MapStatusArgs, reply *MapStatusReply) error {
	Printf(c.prefix, "worker(%d) has processed map task(%d-%s), update status\n", args.Wid, args.Fid, c.files[args.Fid])

	c.mMu.Lock()

	if !c.runningMapTasks.HasAll(args.Fid) {
		c.mMu.Unlock()
		reply.Committed = false
		Printf(c.prefix, "task(%d) not in running queue", args.Fid)
		return nil
	}
	if c.mts[args.Fid].wid != args.Wid {
		Printf(c.prefix, "map task belongs to worker %v not this %v, ignoring...", c.mts[args.Fid].wid, args.Wid)
		c.mMu.Unlock()
		reply.Committed = false
		return nil
	}

	if time.Now().Sub(c.mts[args.Fid].start) > Timeout {
		log.Println("task exceeds max wait time, abadoning...")
		reply.Committed = false
		c.idleMapTasks.PushBack(args.Fid)
	} else {
		log.Println("task within max wait time, accepting...")
		reply.Committed = true
		c.runningMapTasks.Remove(args.Fid)
	}

	c.mMu.Unlock()
	return nil
}

// shuffleTasks starts when map taskscompleted. Paper 5.3
func (c *Coordinator) shuffleTasks() {
	for i := 0; i < c.nReduce; i++ {
		Printf(c.prefix, "push reduce task(%d) to queue\n", i)
		c.idleReduceTasks.PushBack(i)
	}
}

func (c *Coordinator) AssignReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	Printf(c.prefix, "worker(%v) requests for a reduce task\n", args.Wid)

	c.rMu.Lock()

	if c.idleReduceTasks.Len() == 0 && c.runningReduceTasks.Len() == 0 {
		Printf(c.prefix, "all map and reduce tasks completed, tell workers to terminate")
		c.reduceDone = true
		c.rMu.Unlock()
		reply.IdReduce = -1
		reply.Completed = true
		return nil
	}
	Printf(c.prefix, "reduce task status: idle(%d) running(%d)\n", c.idleReduceTasks.Len(), c.runningReduceTasks.Len())
	c.rMu.Unlock() // unlock to allow update idle tasks

	id := -1
	now := time.Now()
	value := c.idleReduceTasks.PopFront()
	if value != nil {
		id = value.(int)
		c.rMu.Lock()
		c.rts[id].start = now
		c.rts[id].wid = args.Wid
		c.runningReduceTasks.Add(id)
		c.rMu.Unlock()
		Printf(c.prefix, "assigns reduce task(%d) at time(%s)\n", id, now.String())
	}

	reply.IdReduce = id
	reply.Completed = false
	reply.NReduce = c.nReduce
	reply.NFile = len(c.files)

	return nil
}

func (c *Coordinator) HandleReduceTaskStatus(args *ReduceStatusArgs, reply *ReduceStatusReply) error {
	Printf(c.prefix, "worker(%d) has processed reduce task(%d), update status\n", args.Wid, args.IdReduce)

	c.rMu.Lock()
	defer c.rMu.Unlock()

	if !c.runningReduceTasks.HasAll(args.IdReduce) {
		Printf(c.prefix, "reduce task(%d) not in running queue, drop it\n", args.IdReduce)
		return nil
	}
	if c.rts[args.IdReduce].wid != args.Wid {
		reply.Committed = false
		Printf(c.prefix, "reduce task(%d) with Wid(%d) not consistent with cached Wid(%d), drop it",
			args.IdReduce, args.Wid, c.rts[args.IdReduce].wid)
		return nil
	}

	if time.Now().Sub(c.rts[args.IdReduce].start) > Timeout {
		reply.Committed = false
		c.idleReduceTasks.PushBack(args.IdReduce)
		Printf(c.prefix, "task exceeds max wait time, abadoning...")
	} else {
		reply.Committed = true
		c.runningReduceTasks.Remove(args.IdReduce)
		Printf(c.prefix, "task within max wait time, accepting...")
	}

	return nil
}

// paper 3.3 Fault Tolerance: worker failure
func (c *Coordinator) ReExecuteTimeoutTasks() {
	for {
		Printf(c.prefix, "re-execute timeout tasks...")
		c.reExecMapTasks(c.runningMapTasks, c.mts, c.idleMapTasks)
		c.reExecReduceTasks(c.runningReduceTasks, c.rts, c.idleReduceTasks)
		time.Sleep(5 * time.Second)
	}
}

func (c *Coordinator) reExecMapTasks(s *set.Set, mts []MapTask, q *queue.LinkedBlockQueue) {
	c.mMu.Lock()

	s.ForEach(func(element set.T) bool {
		if time.Now().Sub(mts[element.(int)].start) > Timeout {
			Printf(c.prefix, "worker %v on file %v abandoned due to timeout\n", mts[element.(int)].wid, element)
			s.Remove(element)
			q.PushBack(element.(int))
		}
		return true
	})

	c.mMu.Unlock()

}

func (c *Coordinator) reExecReduceTasks(s *set.Set, rts []ReduceTask, q *queue.LinkedBlockQueue) {
	c.rMu.Lock()
	s.ForEach(func(element set.T) bool {
		if time.Now().Sub(rts[element.(int)].start) > Timeout {
			log.Printf("worker %v on file %v abandoned due to timeout\n", rts[element.(int)].wid, element)
			s.Remove(element)
			q.PushBack(element.(int))
		}
		return true
	})

	c.rMu.Unlock()
}

func setFlag(reply *MapReply) {
	log.Println("all map tasks complete, telling workers to switch to reduce mode")
	reply.Fid = -1
	reply.Completed = true
}

func Printf(prefix string, format string, v ...interface{}) {
	log.Printf(prefix+format, v...)
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job hascompleted.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.rMu.Lock()
	done := c.reduceDone
	c.rMu.Unlock()
	Printf(c.prefix, "map reduce tasks status: all completed? %v!\n", done)
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:              files,
		nReduce:            nReduce,
		counter:            0,
		idleMapTasks:       queue.NewLinkedBlockQueue(),
		runningMapTasks:    set.NewSet(),
		mMu:                sync.Mutex{},
		idleReduceTasks:    queue.NewLinkedBlockQueue(),
		runningReduceTasks: set.NewSet(),
		rMu:                sync.Mutex{},
		mts:                make([]MapTask, len(files)),
		rts:                make([]ReduceTask, nReduce),
		mapDone:            false,
		reduceDone:         false,
		prefix:             "[Coordinator] ",
	}
	Printf(c.prefix, "created, files(%d) reduces(%d)\n", len(files), nReduce)

	c.server()
	Printf(c.prefix, "server started\n")

	for i := 0; i < len(files); i++ {
		c.idleMapTasks.PushBack(i)
		Printf(c.prefix, "push map task(file-%d) to idle queue\n", i)
	}
	go c.ReExecuteTimeoutTasks()

	return &c
}
