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
	"time"
)

const TaskProcessingTimeout = 10 * time.Second

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
	TaskStateNotReady
)

type Task struct {
	ID             int
	Type           TaskType
	State          TaskState
	WorkerID       string
	Locations      []string
	LastUpdateTime int64
	ReduceTaskID   int
}

type Coordinator struct {
	NReduce int

	Inputs      []string
	MapTasks    []Task
	ReduceTasks []Task

	isReduceStarted bool

	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.Lock()
	defer c.Unlock()

	for i, task := range c.MapTasks {
		// Get the first idle task
		if task.State == TaskStateIdle {
			// assign the task to the worker
			reply.Task = task
			reply.NReduce = c.NReduce

			// set the status to processing
			c.MapTasks[i].WorkerID = args.WorkerID
			c.MapTasks[i].State = TaskStateProcessing
			c.MapTasks[i].LastUpdateTime = time.Now().UnixMilli()

			return nil
		}
	}

	for i, task := range c.ReduceTasks {
		// Get the first idle task
		if task.State == TaskStateIdle {
			// assign the task to the worker
			reply.Task = task
			reply.NReduce = c.NReduce

			// set the status to processing
			c.ReduceTasks[i].WorkerID = args.WorkerID
			c.ReduceTasks[i].State = TaskStateProcessing
			c.ReduceTasks[i].LastUpdateTime = time.Now().UnixMilli()

			return nil
		}
	}

	return errors.New("no idle tasks")
}

func (c *Coordinator) UpdateTaskProgress(args *UpdateTaskProgressArgs, reply *UpdateTaskProgressReply) error {
	c.Lock()
	defer c.Unlock()

	if args.TaskType == TaskTypeMap {
		if c.MapTasks[args.TaskID].WorkerID != args.WorkerID {
			fmt.Printf("invalid map worker ID| taskID=%d, %s != %s\n", args.TaskID, c.MapTasks[args.TaskID].WorkerID, args.WorkerID)
			return errors.New("invalid map worker ID")
		}

		c.MapTasks[args.TaskID].State = args.TaskState
		c.MapTasks[args.TaskID].LastUpdateTime = args.Timestamp

		if args.TaskState == TaskStateFinished {
			reduceLocations := args.ReduceLocations
			// fmt.Printf("map task finished, reduce_locations=%+v\n", reduceLocations)
			for i, location := range reduceLocations {
				if location == "" {
					continue
				}
				c.ReduceTasks[i].Locations = append(c.ReduceTasks[i].Locations, location)
			}
		}
	} else if args.TaskType == TaskTypeReduce {
		if c.ReduceTasks[args.TaskID].WorkerID != args.WorkerID {
			fmt.Printf("invalid reduce worker ID| taskID=%d, %s != %s\n", args.TaskID, c.ReduceTasks[args.TaskID].WorkerID, args.WorkerID)
			return errors.New("invalid reduce worker ID")
		}

		c.ReduceTasks[args.TaskID].State = args.TaskState
		c.ReduceTasks[args.TaskID].LastUpdateTime = args.Timestamp
	}

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

func (c *Coordinator) startCheckProgress() {
	ticker := time.NewTicker(500 * time.Millisecond)

	go func() {
		for t := range ticker.C {
			c.Lock()

			// Check mapper progress, enable reduce task if all finished
			if c.isTasksFinished(c.MapTasks) && !c.isReduceStarted {
				c.enableReduceTasks()
				c.isReduceStarted = true
			}

			// check in progress tasks, reset if timeout
			for i := 0; i < len(c.MapTasks); i++ {
				lastUpdateTime := c.MapTasks[i].LastUpdateTime
				state := c.MapTasks[i].State

				if state == TaskStateProcessing && t.Sub(time.UnixMilli(lastUpdateTime)) > TaskProcessingTimeout {
					// reset it
					c.MapTasks[i].State = TaskStateIdle
					c.MapTasks[i].LastUpdateTime = 0
				}
			}

			c.Unlock()
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()

	if ok := c.isTasksFinished(c.MapTasks); !ok {
		return false
	}

	if !c.isReduceStarted {
		return false
	}

	if ok := c.isTasksFinished(c.ReduceTasks); !ok {
		return false
	}

	fmt.Println("shutting down coordinator")
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:     nReduce,
		Inputs:      files,
		MapTasks:    createMapTasks(files),
		ReduceTasks: createReduceTasks(nReduce),
	}

	c.server()
	c.startCheckProgress()
	return &c
}

func (c *Coordinator) isTasksFinished(tasks []Task) bool {
	for _, task := range tasks {
		if task.State != TaskStateFinished {
			return false
		}
	}

	return true
}

func (c *Coordinator) enableReduceTasks() {
	for i := 0; i < len(c.ReduceTasks); i++ {
		c.ReduceTasks[i].State = TaskStateIdle
	}
}

func createMapTasks(files []string) []Task {
	tasks := make([]Task, 0, len(files))
	for i, file := range files {
		tasks = append(tasks, Task{
			ID:        i,
			Type:      TaskTypeMap,
			State:     TaskStateIdle,
			Locations: []string{file},
		})
	}
	return tasks
}

// createReduceTasks
func createReduceTasks(nReduce int) []Task {
	tasks := make([]Task, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		tasks = append(tasks, Task{
			ID:        i,
			Type:      TaskTypeReduce,
			State:     TaskStateNotReady,
			Locations: make([]string, 0),
		})
	}

	return tasks
}
