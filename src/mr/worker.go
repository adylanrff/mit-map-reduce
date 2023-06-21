package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	for {
		args := &GetTaskArgs{}
		reply := &GetTaskReply{}
		ok := call("Coordinator.GetTask", args, reply)
		if !ok {
			return
		}

		switch reply.Task.Type {
		case TaskTypeMap:
			doMap(mapf, reply.Task.ID, reply.Task.Location, reply.NReduce)
		case TaskTypeReduce:
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
