package mr

import (
	util "6.824/utils"
	"fmt"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

var workerId int

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	//获得worker节点配置基础配置信息
	//利用rpc向master请求任务
	//查看任务状态
	//看情况调用mapf 或者 reducef
	//调用中或完成时统计信息
	//将中间结果写入文件
	//利用rpc向master汇报任务情况

	pid := os.Getpid()

	RegisterNode(strconv.Itoa(pid))
	for {
		ApplyTask(workerId, strconv.Itoa(pid))
	}

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

func RegisterNode(name string) bool {
	args := ConfigReq{name}
	reply := ConfigResp{}

	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		if reply.head.statusCode != 1 {
			util.Error(reply.head.msg)
			return false
		} else {
			util.Success(reply.head.msg, "worker id: ", reply.workerId)
			workerId = reply.workerId
			return true
		}
	} else {
		fmt.Printf("call registernode failed!\n")
	}
	return false
}

func ApplyTask(workerId int, name string) (b bool, j *Job) {
	args := JobReq{}
	reply := JobResp{}
	args.workerId = workerId
	args.workerName = name
	ok := call("Coordinator.ApplyJob", &args, &reply)
	if ok {
		//if reply.head.statusCode != 1 {
		//	util.Error(reply.head.msg)
		//	return false
		//} else {
		//	util.Success(reply.head.msg, "worker id: ", reply.workerId)
		//	workerId = reply.workerId
		//	return true
		//}
	} else {
		fmt.Printf("call applytask failed!\n")
	}
	return false, nil
}

func SendRes() bool {
	return false
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
