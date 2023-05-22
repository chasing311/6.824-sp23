package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Coordinator struct {
	// Your definitions here.
	nMap 						int
	nReduce 				int
	nFinishedMap 		int
	nFinishedReduce int
	mapTasks				[]MapTask
	reduceTasks			[]ReduceTask
	mu							sync.Mutex
}

type MapTask struct {
	file						string
	status					int
}

type ReduceTask struct {
	status 					int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nFinishedMap < c.nMap {
		for i := 0; i < c.nMap; i++ {
			// !!! THIS NOT WORK !!!
			// struct is call-by-value, need pointer to change the origin value
			// task := c.mapTasks[i]

			task := &c.mapTasks[i]
			if task.status == 0 {
				task.status = 1

				// assign map task
				reply.TaskType = 1
				reply.TaskIndex = i
				reply.File = task.file
				reply.NReduce = c.nReduce

				// check worker timeout
				go func(i int) {
					time.Sleep(10 * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()

					if c.mapTasks[i].status != 2 {
						// assume the map worker has dead
						c.mapTasks[i].status = 0
					}
				}(i)

				return nil
			}
		}
	} else if c.nFinishedReduce < c.nReduce {
		for i := 0; i < c.nReduce; i++ {
			task := &c.reduceTasks[i]
			if task.status == 0 {
				task.status = 1

				// assign reduce task
				reply.TaskType = 2
				reply.TaskIndex = i
				reply.NMap = c.nMap

				// check worker timeout
				go func(i int) {
					time.Sleep(10 * time.Second)
					c.mu.Lock()
					defer c.mu.Unlock()

					if c.reduceTasks[i].status != 2 {
						// assume the reduce worker has dead
						c.reduceTasks[i].status = 0
					}
				}(i)

				return nil
			}
		}
	} else {
		// assign exit task
		reply.TaskType = 3
		return nil
	}

	// assign wait task by default
	reply.TaskType = 0
	return nil
}

func (c *Coordinator) TaskFinish(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == 1 {
		c.nFinishedMap++
		c.mapTasks[args.TaskIndex].status = 2
	}
	if args.TaskType == 2 {
		c.nFinishedReduce++
		c.reduceTasks[args.TaskIndex].status = 2
	}

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.nFinishedReduce == c.nReduce

	return ret
}

func (c *Coordinator) init(files []string, nReduce int) {
	c.nMap = len(files)
	c.nReduce = nReduce
	c.nFinishedMap = 0
	c.nFinishedReduce = 0
	c.mapTasks = make([]MapTask, len(files))
	for i, file := range files {
		c.mapTasks[i] = MapTask{ file: file, status: 0 }
	}
	c.reduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{ status: 0 }
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.init(files, nReduce)

	c.server()
	return &c
}
