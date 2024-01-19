package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTaskState struct {
	FinishedIntermediate []int
}

type ReduceTaskState struct {
	ToFetchIntermediate []int
}

type TaskState struct {
	Task Task

	WorkerId     int
	IssueTime    int64
	LastBeatTime int64
	Finished     bool

	MapTaskState    MapTaskState
	ReduceTaskState ReduceTaskState
}

type AbstractMapTaskState struct {
	MapId int

	file                 string
	TaskInstances        []*TaskState
	FinishedIntermediate []bool
	Finished             bool
}
type AbstractReduceTaskState struct {
	ReduceId int

	TaskInstances []*TaskState
	Finished      bool
}

type WorkerState struct {
	WorkerId int

	RegisterTime         int64
	CurrentTask          *TaskState
	CurrentTaskIssueTime int64
	LastBeatTime         int64
	HistoryTasks         []*TaskState
}

type Coordinator struct {
	M, R         int
	WorkerStates []*WorkerState
	TaskStates   []*TaskState
	MapStates    []*AbstractMapTaskState
	ReduceStates []*AbstractReduceTaskState
	Finished     bool

	global_mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Register
func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.global_mutex.Lock()
	defer c.global_mutex.Unlock()
	reply.WorkerId = len(c.WorkerStates)
	currentTime := time.Now().UnixNano()
	c.WorkerStates = append(c.WorkerStates, &WorkerState{
		WorkerId:             reply.WorkerId,
		RegisterTime:         currentTime,
		CurrentTask:          nil,
		CurrentTaskIssueTime: 0,
		LastBeatTime:         currentTime,
		HistoryTasks:         []*TaskState{},
	})
	return nil
}

// Fetch
func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	c.global_mutex.Lock()
	defer c.global_mutex.Unlock()
	workerId := args.WorkerId
	currentTime := time.Now().UnixNano()

	workerState := c.WorkerStates[workerId]

	assignMapTask := func(mapState *AbstractMapTaskState) {
		task := Task{
			TaskType: MapTask,
			TaskId:   len(c.TaskStates),
			MapId:    mapState.MapId,
			ReduceId: -1,
		}
		c.TaskStates = append(c.TaskStates, &TaskState{
			Task:         task,
			WorkerId:     workerId,
			IssueTime:    currentTime,
			LastBeatTime: currentTime,
			Finished:     false,
			MapTaskState: MapTaskState{
				FinishedIntermediate: []int{},
			},
		})

		if workerState.CurrentTask != nil {
			panic("worker's current task is not nil")
		}
		workerState.CurrentTask = c.TaskStates[len(c.TaskStates)-1]
		workerState.CurrentTaskIssueTime = currentTime
		mapState.TaskInstances = append(mapState.TaskInstances, c.TaskStates[len(c.TaskStates)-1])

		reply.Task = task
		reply.M = c.M
		reply.R = c.R
		reply.MapFile = mapState.file
	}
	assignReduceTask := func(reduceState *AbstractReduceTaskState) {
		ToFetchIntermediate := make([]int, c.M)
		for i := 0; i < c.M; i++ {
			ToFetchIntermediate[i] = i
		}

		task := Task{
			TaskType: ReduceTask,
			TaskId:   len(c.TaskStates),
			MapId:    -1,
			ReduceId: reduceState.ReduceId,
		}
		c.TaskStates = append(c.TaskStates, &TaskState{
			Task:         task,
			WorkerId:     workerId,
			IssueTime:    currentTime,
			LastBeatTime: currentTime,
			Finished:     false,
			ReduceTaskState: ReduceTaskState{
				ToFetchIntermediate: ToFetchIntermediate,
			},
		})

		if workerState.CurrentTask != nil {
			panic("worker's current task is not nil")
		}
		workerState.CurrentTask = c.TaskStates[len(c.TaskStates)-1]
		workerState.CurrentTaskIssueTime = currentTime
		reduceState.TaskInstances = append(reduceState.TaskInstances, c.TaskStates[len(c.TaskStates)-1])

		reply.Task = task
		reply.M = c.M
		reply.R = c.R
	}

	// check if there is a task to assign
	// policy:
	// 1. assign a map task if there is any
	// 2. assign a reduce task if there is any
	// 3. assign a backup task if there is any

	// 1. assign a map task if there is any
	for _, mapState := range c.MapStates {
		if len(mapState.TaskInstances) != 0 {
			continue
		}
		assignMapTask(mapState)
		return nil
	}

	// 2. assign a reduce task if there is any
	for _, reduceState := range c.ReduceStates {
		if len(reduceState.TaskInstances) != 0 {
			continue
		}
		assignReduceTask(reduceState)
		return nil
	}

	timeout_sec := 10
	// 3. assign a backup task if there is any
	for _, mapState := range c.MapStates {
		if mapState.Finished {
			continue
		}
		if len(mapState.TaskInstances) == 0 {
			panic("mapState.TaskInstances is empty")
		}
		lastIssueTime := mapState.TaskInstances[len(mapState.TaskInstances)-1].IssueTime
		if currentTime-lastIssueTime > int64(timeout_sec)*int64(time.Second) {
			assignMapTask(mapState)
			return nil
		}
	}
	// 4. assign a backup task if there is any
	for _, reduceState := range c.ReduceStates {
		if reduceState.Finished {
			continue
		}
		if len(reduceState.TaskInstances) == 0 {
			panic("reduceState.TaskInstances is empty")
		}
		lastIssueTime := reduceState.TaskInstances[len(reduceState.TaskInstances)-1].IssueTime
		if currentTime-lastIssueTime > int64(timeout_sec)*int64(time.Second) {
			assignReduceTask(reduceState)
			return nil
		}
	}
	// 5. no task to assign
	reply.Task = Task{
		TaskType: WaitTask,
	}
	return nil
}

// FinishTask
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.global_mutex.Lock()
	defer c.global_mutex.Unlock()
	workerId := args.WorkerId
	task := args.Task

	c.TaskStates[task.TaskId].Finished = true

	c.WorkerStates[workerId].CurrentTask = nil
	c.WorkerStates[workerId].CurrentTaskIssueTime = 0
	c.WorkerStates[workerId].HistoryTasks = append(c.WorkerStates[workerId].HistoryTasks, c.TaskStates[task.TaskId])

	if task.TaskType == MapTask {
		c.MapStates[task.MapId].Finished = true
	} else {
		c.ReduceStates[task.ReduceId].Finished = true
	}
	return nil
}

// Heartbeat
func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.global_mutex.Lock()
	defer c.global_mutex.Unlock()
	workerId := args.WorkerId
	task := args.Task
	currentTime := time.Now().UnixNano()

	c.WorkerStates[workerId].LastBeatTime = currentTime
	if c.WorkerStates[workerId].CurrentTask != c.TaskStates[task.TaskId] {
		panic("worker's current task is not the same as the task in heartbeat")
	}
	c.TaskStates[task.TaskId].LastBeatTime = currentTime
	return nil
}

// FetchIntermediate
func (c *Coordinator) FetchIntermediate(args *FetchIntermediateArgs, reply *FetchIntermediateReply) error {
	c.global_mutex.Lock()
	defer c.global_mutex.Unlock()
	workerId := args.WorkerId
	task := args.Task

	if c.WorkerStates[workerId].CurrentTask != c.TaskStates[task.TaskId] {
		panic("worker's current task is not the same as the task in heartbeat")
	}

	reduceId := task.ReduceId
	taskState := c.TaskStates[task.TaskId].ReduceTaskState
	// for mapId in taskState.ToFetchIntermediate
	new_to_fetch_intermediate := []int{}
	for _, mapId := range taskState.ToFetchIntermediate {
		if c.MapStates[mapId].FinishedIntermediate[reduceId] == true {
			reply.IntermediateFiles = append(reply.IntermediateFiles, mapId)
		} else {
			new_to_fetch_intermediate = append(new_to_fetch_intermediate, mapId)
		}
	}
	taskState.ToFetchIntermediate = new_to_fetch_intermediate

	return nil
}

// FinishIntermediate
func (c *Coordinator) FinishIntermediate(args *FinishIntermediateArgs, reply *FinishIntermediateReply) error {
	c.global_mutex.Lock()
	defer c.global_mutex.Unlock()
	workerId := args.WorkerId
	task := args.Task

	if c.WorkerStates[workerId].CurrentTask != c.TaskStates[task.TaskId] {
		panic("worker's current task is not the same as the task in heartbeat")
	}

	mapId := task.MapId
	reduceId := args.ReduceId
	mapTaskState := c.TaskStates[task.TaskId].MapTaskState
	mapTaskState.FinishedIntermediate = append(mapTaskState.FinishedIntermediate, reduceId)
	c.MapStates[mapId].FinishedIntermediate[reduceId] = true

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.global_mutex.Lock()
	defer c.global_mutex.Unlock()
	for _, reduceState := range c.ReduceStates {
		if reduceState.Finished == false {
			return false
		}
	}
	c.Finished = true
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.M = len(files)
	c.R = nReduce
	c.Finished = false

	// initialize MapStates: []*AbstractMapTaskState
	for i := 0; i < c.M; i++ {
		c.MapStates = append(c.MapStates, &AbstractMapTaskState{
			MapId:                i,
			file:                 files[i],
			TaskInstances:        []*TaskState{},
			FinishedIntermediate: make([]bool, c.R),
			Finished:             false,
		})
	}
	// initialize ReduceStates: []*AbstractReduceTaskState
	for i := 0; i < c.R; i++ {
		c.ReduceStates = append(c.ReduceStates, &AbstractReduceTaskState{
			ReduceId:      i,
			TaskInstances: []*TaskState{},
			Finished:      false,
		})
	}
	// initialize TaskStates: []*TaskState
	c.TaskStates = []*TaskState{}
	// initialize WorkerStates: []*WorkerState
	c.WorkerStates = []*WorkerState{}

	c.server()
	return &c
}
