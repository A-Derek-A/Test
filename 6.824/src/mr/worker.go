package mr

import (
	util "6.824/utils"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

var workerId int
var name string

var basepath = "../main/mr-tmp/mr-inters"

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
	_, err := os.Stat(basepath)
	if err != nil {
		if os.IsNotExist(err) {
			os.Mkdir(basepath, 0755)
		}
	}
	name = strconv.Itoa(os.Getpid())

	RegisterNode(name)
	for {
		cor, j, f := ApplyTask(workerId, name)
		if cor == true {
			if f == 1 {
				return
			}
			if j != nil {
				intermediate := make([][]KeyValue, j.ParNum)
				if j.JobType == MapType {
					file, err := os.Open(basepath + "/" + j.JobName)
					defer file.Close()
					if err != nil {
						//util.Error("err: ",err)
						log.Fatal("err: ", err)
					}
					content, err := io.ReadAll(file)
					kva := mapf(file.Name(), string(content))
					if err != nil {
						util.Error("err: ", err)
						log.Fatal("err: ", err)
					}
					for _, v := range kva {
						parid := ihash(v.Key) % j.ParNum
						intermediate[parid] = append(intermediate[parid], v)
					}
					for i := 0; i < len(intermediate); i++ {
						sort.Sort(ByKey(intermediate[i]))
					}
					for i := 0; i < len(intermediate); i++ {
						newFileName := "mr-mid-" + strconv.Itoa(j.JobId) + "-" + strconv.Itoa(i) + ".txt"
						newFile, err := os.OpenFile(basepath+"/"+newFileName, os.O_WRONLY|os.O_CREATE, 0666)
						if err != nil {
							log.Fatal("err: ", err)
						}
						for ind := 0; ind < len(intermediate[i]); ind++ {
							fmt.Fprintf(newFile, "%+v %+v\n", intermediate[i][ind].Key, intermediate[i][ind].Value)
						}
						newFile.Close()
					}
					SendRes() //wait to deal
				} else if j.JobType == ReduceType {
					files, err := os.ReadDir(basepath)
					if err != nil {
						fmt.Printf("err: ", err)
					}
					for _, file := range files {
						if file.Name()[len(file.Name())-5:] == strconv.Itoa(j.JobId)+".txt" {

						}
					}
				}
			}
		}
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
	yes := call("Coordinator.Example", &args, &reply)
	if yes {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func RegisterNode(name string) bool {
	args := ConfigReq{name}
	reply := ConfigResp{}

	yes := call("Coordinator.Register", &args, &reply)
	if yes {
		if reply.head.statusCode == ok {
			util.Success(reply.head.msg, "worker id: ", reply.workerId)
			workerId = reply.workerId
			return true
		} else if reply.head.statusCode == mistake {
			util.Error(reply.head.msg)
			return false
		}
	} else {
		fmt.Printf("call registernode failed!\n")
	}
	return false
}

func ApplyTask(workerId int, name string) (cor bool, j *Job, flag int) { //cor 标志是否正确执行 j 是回传的job类 flag 标记是退出主程序
	args := JobReq{}
	reply := JobResp{}
	args.workerId = workerId
	args.workerName = name
	flag = 0
	yes := call("Coordinator.ApplyJob", &args, &reply)
	if yes {
		if reply.head.statusCode == ok {
			j = reply.job
			cor = true
			util.Success("worker %+v: %+v", name, reply.head.msg)
			return cor, j, flag
		} else if reply.head.statusCode == nojob {
			j = nil
			cor = true
			util.Info("worker %+v: %+v", name, reply.head.msg)
			time.Sleep(time.Second)
			return cor, j, flag
		} else if reply.head.statusCode == exit {
			return true, nil, 1
		} else if reply.head.statusCode == mistake {
			util.Error("worker %+v: %+v", name, reply.head.msg)
		}
	} else {
		fmt.Printf("call applytask failed!\n")
	}
	return false, nil, 0
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
