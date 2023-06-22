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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	var (
		currentTask      Task
		nReduce          int
		isProcessingTask bool

		errChan  = make(chan error)
		workerID = uuid.NewString()
	)

	defer close(errChan)

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
					errChan <- doMap(mapf, currentTask.ID, currentTask.Location, nReduce)
				case TaskTypeReduce:
					errChan <- doReduce(reducef, currentTask.ReduceTaskID, currentTask.Location)
				case TaskTypeShutdown:
					fmt.Println("shutting down")
					return
				}
			}()

			// Report that the task has been finished
			for {
				select {
				case err := <-errChan:
					if err != nil {
						// Set task to idle again
						_ = callUpdateTaskProgress(workerID, currentTask.ID, TaskStateIdle)
					} else {
						//  set task to successful
						_ = callUpdateTaskProgress(workerID, currentTask.ID, TaskStateFinished)
					}
					isProcessingTask = false
				case <-time.After(time.Second):
					// Report that the task is still processing
					_ = callUpdateTaskProgress(workerID, currentTask.ID, TaskStateProcessing)
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

func doMap(mapf func(string, string) []KeyValue, taskID int, location string, nReduce int) error {
	buckets := make(map[int][]KeyValue, nReduce)
	content := readFile(location)
	kva := mapf(location, string(content))
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		if _, ok := buckets[bucket]; !ok {
			buckets[bucket] = make([]KeyValue, 0)
		}
		buckets[bucket] = append(buckets[bucket], kv)
	}

	for bucket, kvs := range buckets {
		file, err := ioutil.TempFile("", "mr-temp-*")
		if err != nil {
			return err
		}

		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			enc.Encode(&kv)
		}

		filename := fmt.Sprintf("mr-%d-%d", taskID, bucket+1)
		os.Rename(file.Name(), filename)
	}

	return nil
}

func doReduce(reducef func(string, []string) string, reduceTask int, location string) error {
	kva := make([]KeyValue, 0)
	file, err := os.Open(location)
	if err != nil {
		log.Fatalf("cannot open %v", location)
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

	sort.Sort(ByKey(kva))

	ofile, err := ioutil.TempFile("", "mr-temp-*")
	if err != nil {
		return err
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

	os.Rename(file.Name(), fmt.Sprintf("mr-out-%d", reduceTask))
	return nil
}

func callUpdateTaskProgress(workerID string, taskID int, taskState TaskState) error {
	args := &UpdateTaskProgressArgs{
		WorkerID:  workerID,
		TaskID:    taskID,
		TaskState: taskState,
		Timestamp: time.Now().Unix(),
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
