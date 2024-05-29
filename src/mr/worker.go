package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

//Worker需要具有以下功能：
//	空闲时向coordinator发送RPC请求，接下Task或汇报Task完成情况

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	id := strconv.Itoa(os.Getegid())
	log.Printf("Worker %s started\n", id)

	taskType := ""
	taskIndex := -1
	for {
		args := MapReduceArgs{
			WorkerID:  id,
			TaskType:  taskType,
			TaskIndex: taskIndex,
		}
		reply := MapReduceReply{}

		ok := call("Coordinator.RPCHandler", &args, &reply)
		if ok {
			taskType = reply.TaskType
			taskIndex = reply.TaskIndex
			switch taskType {
			case MAP:
				//we have a Map task to do
				file, err := os.Open(reply.MapInputFile)
				if err != nil {
					log.Fatalf("Failed to open map input file %s: %e", reply.MapInputFile, err)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read map input file %s: %e", reply.MapInputFile, err)
				}
				file.Close()
				kva := mapf(reply.MapInputFile, string(content))
				hashedKva := make(map[int][]KeyValue)
				for _, kv := range kva {
					hashedKey := ihash(kv.Key) % reply.ReduceNum
					hashedKva[hashedKey] = append(hashedKva[hashedKey], kv)
				}
				for i := 0; i < reply.ReduceNum; i++ {
					ofile, _ := os.Create(tmpMapOutFile(id, reply.TaskIndex, i))
					for _, kv := range hashedKva[i] {
						fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
					}
					ofile.Close()
				}
			case REDUCE:
				//we have a Reduce task to do
				var lines []string
				for i := 0; i < reply.MapNum; i++ {
					filename := finalMapOutFile(i, reply.TaskIndex)
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("Failed to open map input file %s: %e", filename, err)
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("Failed to read map input file %s: %e", filename, err)
					}
					file.Close()
					lines = append(lines, strings.Split(string(content), "\n")...)
				}
				var kva []KeyValue
				for _, line := range lines {
					if strings.TrimSpace(line) == "" {
						continue
					}
					pair := strings.Split(line, "\t")
					kva = append(kva, KeyValue{
						Key:   pair[0],
						Value: pair[1],
					})
				}
				sort.Sort(ByKey(kva))
				ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskIndex))
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

				ofile.Close()
			default:
				//no task
				break
			}
		}
		log.Printf("Worker %s exit\n", id)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
