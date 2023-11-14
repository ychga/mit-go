package mr

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//Coordinator需要具有以下功能：
//	根据输入文件和reduce个数生成MapTask和ReduceTask
//	响应Worker的RPC请求，分配给它Task或从它的请求中得知Task的完成结果
//	能够知晓超时的Task，未完成的Task将被重新执行，还要保证只有一个Worker最终输出Task结果

type Task struct {
	Type         string
	Index        int
	MapInputFile string
	WorkerID     string
	Deadline     time.Time
}

type Coordinator struct {
	// Your definitions here.
	lock           sync.Mutex
	Stage          string //{"MAP", "REDUCE"}
	MapNum         int
	ReduceNum      int
	Tasks          map[string]Task
	AvailableTasks chan Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandler(args *MapReduceArgs, reply *MapReduceReply) error {
	if args.TaskType != "" {
		//LastTask finished
		c.lock.Lock()
		taskID := GenTaskID(args.TaskType, args.TaskIndex)
		if task, exists := c.Tasks[taskID]; exists && task.WorkerID == args.WorkerID {
			switch task.Type {
			case MAP:
				//a Map task finished
				for i := 0; i < c.ReduceNum; i++ {
					err := os.Rename(
						tmpMapOutFile(args.WorkerID, args.TaskIndex, i),
						finalMapOutFile(args.TaskIndex, i))
					if err != nil {
						log.Fatalf(
							"Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(args.WorkerID, args.TaskIndex, i), err)
					}
				}
			case REDUCE:
				//a Reduce task finished
				err := os.Rename(
					tmpReduceOutFile(args.WorkerID, args.TaskIndex),
					finalReduceOutFile(args.TaskIndex))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(args.WorkerID, args.TaskIndex), err)
				}
			default:
				//
			}

			delete(c.Tasks, taskID)
			if len(c.Tasks) == 0 {
				c.transit()
			}
		}
		c.lock.Unlock()
	}

	//Assigin a Task to Worker
	task, ok := <-c.AvailableTasks
	if ok {
		c.lock.Lock()
		defer c.lock.Unlock()
		task.WorkerID = args.WorkerID
		task.Deadline = time.Now().Add(10 * time.Second)
		c.Tasks[GenTaskID(task.Type, task.Index)] = task
		reply.TaskType = task.Type
		reply.TaskIndex = task.Index
		reply.MapInputFile = task.MapInputFile
		reply.MapNum = c.MapNum
		reply.ReduceNum = c.ReduceNum

		return nil
	}
	return nil
}

func (c *Coordinator) transit() {
	switch c.Stage {
	case MAP:
		//generate Reduce tasks and go to REDUCE
		c.Stage = REDUCE
		for i := 0; i < c.ReduceNum; i++ {
			task := Task{
				Type:  REDUCE,
				Index: i,
			}
			c.Tasks[GenTaskID(task.Type, task.Index)] = task
			c.AvailableTasks <- task
		}
	case REDUCE:
		//shut down coordinator's channel
		c.Stage = ""
		close(c.AvailableTasks)
	default:
		//
	}
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

	// Your code here.
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Stage == ""
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Stage:          MAP,
		MapNum:         len(files),
		ReduceNum:      nReduce,
		Tasks:          make(map[string]Task),
		AvailableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	//创建MapTask
	for i, file := range files {
		task := Task{
			Type:         MAP,
			Index:        i,
			MapInputFile: file,
		}
		c.Tasks[GenTaskID(task.Type, task.Index)] = task
		c.AvailableTasks <- task
	}

	c.server()

	// 启用Task回收机制
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.Tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					log.Printf(
						"Found timed-out %s task %d previously running on worker %s. Prepare to re-assign",
						task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.AvailableTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()

	return &c
}

func GenTaskID(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}
