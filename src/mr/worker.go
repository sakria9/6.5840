package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	retry := 0
	max_retry := 2

	// 1. register with coordinator
	workerId := 0

	wait_sec := func() {
		time.Sleep(time.Second)
	}

	for {
		if register(&workerId) {
			retry = 0
			break
		}
		retry++
		wait_sec()
		if retry == max_retry {
			log.Fatal("register failed!")
			return
		}
	}

	// 2. fetch task from coordinator
	for {
		reply := FetchTaskReply{}
		for {
			if fetchTask(workerId, &reply) {
				retry = 0
				break
			}
			retry++
			wait_sec()
			if retry == max_retry {
				// log.Fatal("fetch task failed!")
				log.Printf("fetch task failed: %v\n", reply)
				return
			}
		}

		// 3. execute task
		switch reply.Task.TaskType {
		case MapTask:
			do_map(workerId, &reply, mapf)
		case ReduceTask:
			do_reduce(workerId, &reply, reducef)
		case WaitTask:
			wait_sec()
		case ExitTask:
			return // exit
		default:
			panic("unknown task type")
		}
	}
}

func do_map(workerId int, reply *FetchTaskReply, mapf func(string, string) []KeyValue) {
	task := reply.Task
	mapId := task.MapId
	filename := reply.MapFile

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		panic("cannot open file")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		panic("cannot read file")
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	intermediate_buckets := make([][]KeyValue, reply.R)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % reply.R
		intermediate_buckets[bucket] = append(intermediate_buckets[bucket], kv)
	}

	to_report := []int{}
	for reduceId, bucket := range intermediate_buckets {
		// write to a tempfile
		tempfile, err := os.CreateTemp("./", "mr-tmp-*")
		if err != nil {
			log.Fatal(err)
			panic("cannot create tempfile")
		}
		enc := json.NewEncoder(tempfile)
		for _, kv := range bucket {
			enc.Encode(&kv)
		}

		tempfile.Close()
		// rename tempfile to final output file
		oname := fmt.Sprintf("mr-%v-%v", mapId, reduceId)
		os.Rename(tempfile.Name(), oname)
		// report to coordinator
		if finishIntermediate(workerId, task, reduceId) {
		} else {
			to_report = append(to_report, reduceId)
		}
	}

	for _, reduceId := range to_report {
		if finishIntermediate(workerId, task, reduceId) {
		} else {
			panic("finish intermediate failed!")
		}
	}

	if finishTask(workerId, task) {
	} else {
		panic("finish task failed!")
	}
}

func do_reduce(workerId int, reply *FetchTaskReply, reducef func(string, []string) string) {
	task := reply.Task
	reduceId := task.ReduceId

	retry := 0

	reducedCount := 0
	kv := map[string][]string{}
	for reducedCount < reply.M {
		intermediateFiles := FetchIntermediate(workerId, task)
		if intermediateFiles == nil {
			retry++
			if retry == 10 {
				panic("fetch intermediate failed!")
			}
			time.Sleep(time.Second)
			continue
		} else if len(intermediateFiles) == 0 {
			time.Sleep(time.Second)
		}
		retry = 0

		for _, intermediateFile := range intermediateFiles {
			filename := fmt.Sprintf("mr-%v-%v", intermediateFile, reduceId)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				panic("cannot open file")
			}
			dec := json.NewDecoder(file)
			for {
				var kv_tmp KeyValue
				if err := dec.Decode(&kv_tmp); err != nil {
					break
				}
				kv[kv_tmp.Key] = append(kv[kv_tmp.Key], kv_tmp.Value)
			}
			file.Close()
		}
		reducedCount += len(intermediateFiles)
	}

	kva := []KeyValue{}
	for k, v := range kv {
		kva = append(kva, KeyValue{k, reducef(k, v)})
	}
	sort.Sort(ByKey(kva))

	tempfile, err := os.CreateTemp("./", "mr-tmp-*")
	if err != nil {
		log.Fatal(err)
		panic("cannot create tempfile")
	}

	for _, kv := range kva {
		fmt.Fprintf(tempfile, "%v %v\n", kv.Key, kv.Value)
	}

	tempfile.Close()
	os.Rename(tempfile.Name(), fmt.Sprintf("mr-out-%v", reduceId))

	if finishTask(workerId, task) {
	} else {
		panic("finish task failed!")
	}
}

func register(ret *int) bool {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		*ret = reply.WorkerId
		return true
	} else {
		// log.Fatal("register failed!")
		log.Printf("register failed: %v\n", args)
		return false
	}
}

func fetchTask(workerId int, reply *FetchTaskReply) bool {
	args := FetchTaskArgs{workerId}
	ok := call("Coordinator.FetchTask", &args, &reply)
	if ok {
		log.Printf("[%v] fetch task: %v\n", workerId, reply)
		return true
	} else {
		// log.Fatal("fetch task failed!")
		log.Printf("[%v] fetch task failed: %v\n", workerId, args)
		return false
	}
}

func finishIntermediate(workerId int, task Task, reduceId int) bool {
	args := FinishIntermediateArgs{workerId, task, reduceId}
	reply := FinishIntermediateReply{}
	ok := call("Coordinator.FinishIntermediate", &args, &reply)
	if ok {
		log.Printf("[%v] finish intermediate: %v\n", workerId, args)
		return true
	} else {
		// log.Fatal("finish intermediate failed!")
		log.Printf("[%v] finish intermediate failed: %v\n", workerId, args)
		return false
	}
}

func FetchIntermediate(workerId int, task Task) []int {
	args := FetchIntermediateArgs{workerId, task}
	reply := FetchIntermediateReply{}
	ok := call("Coordinator.FetchIntermediate", &args, &reply)
	if ok {
		log.Printf("[%v] fetch intermediate: %v %v\n", workerId, args, reply)
		return reply.IntermediateFiles
	} else {
		// log.Fatal("fetch intermediate failed!")
		log.Printf("[%v] fetch intermediate failed: %v\n", workerId, args)
		return nil
	}
}

func finishTask(workerId int, task Task) bool {
	args := FinishTaskArgs{workerId, task}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		log.Printf("[%v] finish task: %v\n", workerId, args)
		return true
	} else {
		// log.Fatal("finish task failed!")
		log.Printf("[%v] finish task failed: %v\n", workerId, args)
		return false
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		log.Printf("dialing: %v\n", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
