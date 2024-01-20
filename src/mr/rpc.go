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
type TaskType string

const (
	MapTask    TaskType = "map"
	ReduceTask TaskType = "reduce"
	ExitTask   TaskType = "exit"
	WaitTask   TaskType = "wait"
)

type Task struct {
	TaskType TaskType // "map" or "reduce"
	TaskId   int      // unique id for each task
	MapId    int      // in [0, M-1]
	ReduceId int      // in [0, R-1]
}

type RegisterArgs struct {
}
type RegisterReply struct {
	WorkerId int
}

type FetchTaskArgs struct {
	WorkerId int
}
type FetchTaskReply struct {
	Task Task
	M, R int
	// MapTask
	MapFile string
	// ReduceTask
}

type FinishTaskArgs struct {
	WorkerId int
	Task     Task
}
type FinishTaskReply struct {
}

type HeartbeatArgs struct {
	WorkerId int
}
type HeartbeatReply struct {
}

type FetchIntermediateArgs struct {
	WorkerId int
	Task     Task
}
type FetchIntermediateReply struct {
	AbortCurrentTask  bool
	IntermediateFiles []int
}

type FinishIntermediateArgs struct {
	WorkerId int
	Task     Task
	ReduceId int
}
type FinishIntermediateReply struct {
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
