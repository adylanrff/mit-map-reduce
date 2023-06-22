package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskType int

const (
	TaskTypeMap = iota + 1
	TaskTypeReduce
	TaskTypeShutdown
)

type TaskState int

const (
	TaskStateIdle = iota + 1
	TaskStateProcessing
	TaskStateFinished
)

type Task struct {
	ID             int
	Type           TaskType
	State          TaskState
	WorkerID       string
	Location       string
	LastUpdateTime int64
	ReduceTaskID   int
}

type Coordinator struct {
	NReduce int
	tasks   []Task

	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Lock()
	defer c.Unlock()

	for i, task := range c.tasks {
		// Get the first idle task
		if task.State == TaskStateIdle {
			// assign the task to the worker
			reply.Task = task
			reply.NReduce = c.NReduce

			// set the status to processing
			c.tasks[i].WorkerID = args.WorkerID
			c.tasks[i].State = TaskStateProcessing

			return nil
		}
	}

	return errors.New("no idle tasks")
}

func (c *Coordinator) UpdateTaskProgress(args *UpdateTaskProgressArgs, reply *UpdateTaskProgressReply) error {
	c.Lock()
	defer c.Unlock()

	if c.tasks[args.TaskID].WorkerID != args.WorkerID {
		return errors.New("invalid worker ID")
	}

	c.tasks[args.TaskID].State = args.TaskState
	c.tasks[args.TaskID].LastUpdateTime = args.Timestamp

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	for _, task := range c.tasks {
		if task.State != TaskStateFinished {
			return false
		}
	}

	fmt.Println("shutting down coordinator")
	return true
}

func createMapTasks(files []string) []Task {
	tasks := make([]Task, 0, len(files))
	for i, file := range files {
		tasks = append(tasks, Task{
			ID:       i,
			Type:     TaskTypeMap,
			State:    TaskStateIdle,
			Location: file,
		})
	}
	return tasks
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce: nReduce,
		tasks:   createMapTasks(files),
	}

	c.server()
	return &c
}
