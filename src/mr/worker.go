package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := RequestTask()
		switch reply.TaskType {
			case 0:
				doWaitTask()
			case 1:
				doMapTask(reply, mapf)
			case 2:
				doReduceTask(reply, reducef)
			case 3:
				doExitTask()
				return
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func RequestTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.Task", &args, &reply)
	if ok {
		return reply
	}

	// If something wrong with coordinator, exit worker
	return TaskReply{ TaskType: 3 }
}

func ReportTask(task TaskReply) {
	args := TaskArgs{ TaskType: task.TaskType, TaskIndex: task.TaskIndex }
	reply := TaskReply{}
	ok := call("Coordinator.TaskFinish", &args, &reply)
	if !ok {
		ReportTask(task)
	}
}

func doWaitTask()  {
	time.Sleep(1 * time.Second)
}

func doMapTask(reply TaskReply, mapf func(string, string) []KeyValue)  {
	filename := reply.File
	taskIndex := reply.TaskIndex
	nReduce := reply.NReduce
	
	// accumulate the intermediate Map output
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// generate buckets for reduce
	buckets := make([][]KeyValue, nReduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	for _, kva := range intermediate {
		bucketIndex := ihash(kva.Key) % nReduce
		buckets[bucketIndex] = append(buckets[bucketIndex], kva)
	}
	for i, bucket := range buckets {
		oname := fmt.Sprintf("mr-%d-%d", taskIndex, i)
		// create a temp file to make sure is atomic
		ofile, _ := os.CreateTemp("", oname+"*")
		defer ofile.Close()

		enc := json.NewEncoder(ofile)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write into %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
	}

	ReportTask(reply)
}

func doReduceTask(reply TaskReply, reducef func(string, []string) string) {
	taskIndex := reply.TaskIndex
	nMap := reply.NMap

	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskIndex)
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", taskIndex)
	ofile, _ := os.CreateTemp("", oname+"*")
	defer ofile.Close()

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)

	// remove intermediate files
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskIndex)
		err := os.Remove(filename)
		if err != nil {
			log.Fatalf("cannot delete %v", filename)
		}
	}

	ReportTask(reply)
}

func doExitTask() {
	fmt.Println("task finish, exit now...")
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
