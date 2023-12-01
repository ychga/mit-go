package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
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

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
)

// Add your RPC definitions here.
type MapReduceArgs struct {
	WorkerID  string
	TaskIndex int
	TaskType  string //{"MAP","REDUCE",""}
}

type MapReduceReply struct {
	TaskIndex    int
	TaskType     string
	MapInputFile string
	MapNum       int
	ReduceNum    int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func tmpMapOutFile(workerID string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-%d-%d", workerID, mapIndex, reduceIndex)
}

func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func tmpReduceOutFile(workerID string, reduceIndex int) string {
	return fmt.Sprintf("tmp-worker-%s-out-%d", workerID, reduceIndex)
}

func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}
