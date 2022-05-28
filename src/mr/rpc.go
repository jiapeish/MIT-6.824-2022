package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MapArgs struct {
	Wid int
}

type MapReply struct {
	Filename string
	Fid      int // also represents map task id
	Wid      int
	NReduce  int

	Completed bool
}

type ReduceArgs struct {
	Wid int
}

type ReduceReply struct {
	IdReduce  int // id, one for each reduce task
	NReduce   int // total number of reduce tasks
	NFile     int // total number of files
	Completed bool
}

type MapStatusArgs struct {
	Fid int
	Wid int
}

type MapStatusReply struct {
	Committed bool
}

type ReduceStatusArgs struct {
	Wid      int
	IdReduce int
}

type ReduceStatusReply struct {
	Committed bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
