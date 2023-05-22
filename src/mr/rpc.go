package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type TaskArgs struct {
	// 1-map 2-reduce
	TaskType			int
	TaskIndex			int
}

type TaskReply struct {
	// 0-wait 1-map 2-reduce 3-exit
	TaskType			int
	TaskIndex			int

	// for map task
	File					string
	NReduce				int

	// for reduce task
	NMap					int
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
