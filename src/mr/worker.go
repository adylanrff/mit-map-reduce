package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

type WorkerResult struct {
	err             error
	resultLocations []string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var (
		currentTask      Task
		nReduce          int
		isProcessingTask bool

		workerResultChan = make(chan WorkerResult)
		workerID         = uuid.NewString()
	)

	defer close(workerResultChan)

	for {
		if !isProcessingTask {
			args := &GetTaskArgs{WorkerID: workerID}
			reply := &GetTaskReply{}
			ok := call("Coordinator.GetTask", args, reply)
			if !ok {
				time.Sleep(time.Second)
				continue
			}
			isProcessingTask = true
			currentTask = reply.Task
			nReduce = reply.NReduce
		} else {
			go func() {
				switch currentTask.Type {
				case TaskTypeMap:
					workerResultChan <- doMap(mapf, currentTask.ID, currentTask.Locations, nReduce)
				case TaskTypeReduce:
					workerResultChan <- doReduce(reducef, currentTask.ID, currentTask.Locations)
				case TaskTypeShutdown:
					fmt.Println("shutting down")
					return
				}
			}()

			// Report that the task has been finished
			for {
				select {
				case workerResult := <-workerResultChan:
					err := workerResult.err
					if err != nil {
						// Set task to idle again
						fmt.Printf("task error. taskType %d, err %+v\n", currentTask.Type, err)
						_ = callUpdateTaskProgress(workerID, currentTask, TaskStateIdle, nil)
					} else {
						//  set task to successful
						_ = callUpdateTaskProgress(workerID, currentTask, TaskStateFinished, workerResult.resultLocations)
					}
					isProcessingTask = false
				case <-time.After(time.Second):
					// Report that the task is still processing
					_ = callUpdateTaskProgress(workerID, currentTask, TaskStateProcessing, nil)
				}

				if !isProcessingTask {
					break
				}
			}
		}
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

func doMap(mapf func(string, string) []KeyValue, mapTaskID int, locations []string, nReduce int) WorkerResult {
	var workerResult WorkerResult
	reduceTasks := make(map[int][]KeyValue, nReduce)
	resultLocations := make([]string, nReduce)

	for _, location := range locations {
		content := readFile(location)
		kva := mapf(location, string(content))
		for _, kv := range kva {
			reduceTaskID := ihash(kv.Key) % nReduce
			if _, ok := reduceTasks[reduceTaskID]; !ok {
				reduceTasks[reduceTaskID] = make([]KeyValue, 0)
			}
			reduceTasks[reduceTaskID] = append(reduceTasks[reduceTaskID], kv)
		}
	}

	for reduceTaskID, kvs := range reduceTasks {
		if len(kvs) > 0 {
			file, err := ioutil.TempFile("", "mr-temp-*")
			if err != nil {
				workerResult.err = err
				return workerResult
			}

			enc := json.NewEncoder(file)
			for _, kv := range kvs {
				enc.Encode(&kv)
			}

			filename := fmt.Sprintf("mr-%d-%d", mapTaskID, reduceTaskID)
			os.Rename(file.Name(), filename)
			resultLocations[reduceTaskID] = filename
		}
	}

	workerResult.resultLocations = resultLocations
	return workerResult
}

func doReduce(reducef func(string, []string) string, reduceTask int, locations []string) WorkerResult {
	var workerResult WorkerResult
	kva := make([]KeyValue, 0)

	for _, location := range locations {
		file, err := os.Open(location)
		if err != nil {
			fmt.Printf("cannot open file: %s\n", location)
			workerResult.err = err
			return workerResult
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	ofile, err := ioutil.TempFile("", "mr-temp-*")
	if err != nil {
		workerResult.err = err
		return workerResult
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reduceTask))
	return workerResult
}

func callUpdateTaskProgress(workerID string, currentTask Task, taskState TaskState, resultLocations []string) error {
	args := &UpdateTaskProgressArgs{
		WorkerID:        workerID,
		TaskType:        currentTask.Type,
		TaskID:          currentTask.ID,
		TaskState:       taskState,
		Timestamp:       time.Now().UnixMilli(),
		ReduceLocations: resultLocations,
	}

	reply := &UpdateTaskProgressReply{}

	ok := call("Coordinator.UpdateTaskProgress", args, reply)
	if !ok {
		return errors.New("error updating task progress")
	}

	return nil
}

func readFile(location string) []byte {
	file, err := os.Open(location)
	if err != nil {
		log.Fatalf("cannot open %v", location)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", location)
	}
	file.Close()
	return content
}
