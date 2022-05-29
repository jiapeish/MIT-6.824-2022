package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync/atomic"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


const (
	RoleEmpty  int32 = 0
	RoleMap    int32 = 1
	RoleReduce int32 = 2
)

type WorkerNode struct {
	role      int32
	wid       int32
	completed chan struct{}
	// done is closed when all task has been completed
	done chan struct{}

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	prefix string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	w := WorkerNode{
		role:      RoleMap, // default as map
		wid:       InvalidWorkerId,
		mapf:      mapf,
		reducef:   reducef,
		completed: make(chan struct{}),
		done:      make(chan struct{}),
		prefix:    "[Worker] ",
	}

	go w.Run()
	select {
	case <-w.done:
		Printf(w.prefix, "all tasks have been completed, return\n")
	}
}

func (w *WorkerNode) Run() {
	t := time.NewTicker(time.Second)
	for {
		select {
		case <-t.C:
			role := atomic.LoadInt32(&w.role)
			switch role {
			case RoleMap:
				go w.executeMap(w.RequestMapTask())
			case RoleReduce:
				go w.executeReduce(w.RequestReduceTask())
			}
		case <-w.completed:
			Printf(w.prefix, "all work is completed, worker node return\n")
			close(w.done)
			return
		}
	}
}

func (w *WorkerNode) executeMap(reply *MapReply) {
	if reply == nil {
		atomic.StoreInt32(&w.role, RoleReduce)
		Printf(w.prefix, "no more map tasks to handle, switch role to reduce\n")
		return
	}

	if reply.Fid == InvalidFileId {
		Printf(w.prefix, "not valid map id, try again later...\n")
		return
	}

	w.MapProcess(reply)

	w.ReportMapTaskStatus(reply.Fid)
}

func (w *WorkerNode) RequestMapTask() *MapReply {
	args := MapArgs{Wid: int(atomic.LoadInt32(&w.wid))}
	reply := MapReply{}

	Printf(w.prefix, "prepare to request map task...\n")
	call("Coordinator.AssignMapTask", &args, &reply)

	atomic.StoreInt32(&w.wid, int32(reply.Wid))
	if reply.Completed {
		Printf(w.prefix, "map tasks completed\n")
		return nil
	}

	if reply.Fid == InvalidFileId {
		Printf(w.prefix, "map tasks not available yet, wait...\n")
		return &reply
	}

	Printf(w.prefix, "assigned map task(%d-%s) by coordinator", reply.Fid, reply.Filename)
	return &reply
}

func (w *WorkerNode) MapProcess(reply *MapReply) {
	Printf(w.prefix, "map process flow, output map results to files\n")

	intermediate := w.localWriteIntermediates(reply.Filename, w.mapf)

	kva := make([][]KeyValue, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		kva[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % reply.NReduce
		kva[index] = append(kva[index], kv)
	}

	for i := 0; i < reply.NReduce; i++ {
		temp, err := os.CreateTemp(".", "mr-temp")
		if err != nil {
			Printf(w.prefix, "map process flow, create temp file failed(%v)\n", err)
			continue
		}

		enc := json.NewEncoder(temp)
		for _, kv := range kva[i] {
			err = enc.Encode(&kv)
			if err != nil {
				Printf(w.prefix, "map process flow, json encode failed(%v)\n", err)
				continue
			}
		}

		out := fmt.Sprintf("mr-%v-%v", reply.Fid, i)
		err = os.Rename(temp.Name(), out)
		if err != nil {
			Printf(w.prefix, "map process flow, rename temp to out(%v) failed(%v)\n", out, err)
		}
	}
}

func (w *WorkerNode) ReportMapTaskStatus(fid int) {
	args := MapStatusArgs{
		Fid: fid,
		Wid: int(atomic.LoadInt32(&w.wid)),
	}
	reply := MapStatusReply{}

	call("Coordinator.HandleMapTaskStatus", &args, &reply)
	Printf(w.prefix, "reported map task(%v) status, coordinator commit status(%v)\n", args, reply.Committed)
}

func (w *WorkerNode) localWriteIntermediates(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		Printf(w.prefix, "open file(%s) failed(%v)\n", filename, err)
		return nil
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		Printf(w.prefix, "read file(%s) failed(%v)\n", filename, err)
		return nil
	}

	return mapf(filename, string(content))
}

func (w *WorkerNode) executeReduce(reply *ReduceReply) {
	if reply == nil {
		w.completed <- struct{}{}
		Printf(w.prefix, "no more reduce tasks to handle, completed\n")
		return
	}

	if reply.IdReduce == -1 {
		Printf(w.prefix, "not valid reduce id, try again later...\n")
		return
	}

	err := w.ReduceProcess(reply, w.reducef)
	if err != nil {
		Printf(w.prefix, "reduce process failed(%v)\n", err)
		return
	}

	w.ReportReduceTaskStatus(reply.IdReduce)
}

func (w *WorkerNode) RequestReduceTask() *ReduceReply {
	args := ReduceArgs{Wid: int(atomic.LoadInt32(&w.wid))}
	reply := ReduceReply{}

	Printf(w.prefix, "id(%d) requests for a reduce task\n", atomic.LoadInt32(&w.wid))
	call("Coordinator.AssignReduceTask", &args, &reply)

	if reply.Completed {
		Printf(w.prefix, "map reduce completed, terminate workers\n")
		return nil
	}
	if reply.IdReduce == -1 {
		Printf(w.prefix, "tasks not available yet, wait...\n")
		return &reply
	}

	Printf(w.prefix, "assigned reduce task(%d) by coordinator", reply.IdReduce)
	return &reply
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (w *WorkerNode) ReduceProcess(reply *ReduceReply, reducef func(string, []string) string) error {
	var intermediate []KeyValue
	for i := 0; i < reply.NFile; i++ {
		Printf(w.prefix, "reduce process flow, producing intermediates for file(%d)\n", i)
		intermediate = append(intermediate, w.remoteReadIntermediates(i, reply.IdReduce)...)
	}
	Printf(w.prefix, "reduce process flow, produced (%d) intermediate key/value pairs\n", len(intermediate))

	temp, err := os.CreateTemp(".", "mr-temp")
	if err != nil {
		Printf(w.prefix, "reduce process flow, cannot create temp for %v\n", reply.IdReduce)
		return err
	}
	defer temp.Close()

	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && (intermediate)[j].Key == (intermediate)[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, (intermediate)[k].Value)
		}
		output := reducef((intermediate)[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, _ = fmt.Fprintf(temp, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	// Paper 3.3, Semantics in the Presence of Failures
	final := fmt.Sprintf("mr-out-%v", reply.IdReduce)
	err = os.Rename(temp.Name(), final)
	if err != nil {
		Printf(w.prefix, "rename temp failed for %v\n", final)
		return err
	}

	return nil
}

func (w *WorkerNode) remoteReadIntermediates(fileId int, reduceId int) []KeyValue {
	name := fmt.Sprintf("mr-%v-%v", fileId, reduceId)
	file, err := os.Open(name)
	if err != nil {
		Printf(w.prefix, "open file(%v) failed(%v)\n", name, err)
		return nil
	}
	defer file.Close()

	kva := make([]KeyValue, 0)
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err = dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func (w *WorkerNode) ReportReduceTaskStatus(id int) {
	args := ReduceStatusArgs{
		Wid:      int(atomic.LoadInt32(&w.wid)),
		IdReduce: id,
	}
	reply := ReduceStatusReply{}

	call("Coordinator.HandleReduceTaskStatus", &args, &reply)
	Printf(w.prefix, "reported reduce task(%v) status, coordinator commit status(%v)\n", args, reply.Committed)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
