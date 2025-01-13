package mr

import (
	util "6.824/utils"
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
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

var BasePath = "./mr-inters"
var OriginFile = ""
var FinalReducePath = "../main/mr-tmp"

func Merge(l1 []KeyValue, l2 []KeyValue) []KeyValue {
	i := 0
	j := 0
	tempList := make([]KeyValue, 0)
	for i < len(l1) && j < len(l2) {
		if l1[i].Key < l2[j].Key {
			tempList = append(tempList, l1[i])
			i++
		} else {
			tempList = append(tempList, l2[j])
			j++
		}
	}
	for ; i < len(l1); i++ {
		tempList = append(tempList, l1[i])
	}
	for ; j < len(l2); j++ {
		tempList = append(tempList, l2[j])
	}
	return tempList
}

func Partition(lists [][]KeyValue) []KeyValue {
	if len(lists) == 1 {
		return lists[0]
	}
	mid := len(lists) >> 1
	return Merge(Partition(lists[:mid]), Partition(lists[mid:]))
}

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
	os.Mkdir("mr-inters", 0777)
	name = strconv.Itoa(os.Getpid())

	RegisterNode(name)

	for {
		cor, j, f := ApplyTask(workerId, name)
		if j != nil {
			util.Info("job info:", j.ParNum, j.JobStatus, j.JobId, j.JobName, j.JobType)
		}
		if cor == true {
			if f == 1 {
				return
			}
			if j != nil {
				intermediate := make([][]KeyValue, j.ParNum)
				if j.JobType == MapType {
					//OriginFile + "/" +
					file, err := os.Open(j.JobName)
					if err != nil {
						util.Error("err: ", err)
						//log.Fatal("err: ", err)
					}

					content, err := io.ReadAll(file)
					kva := mapf(file.Name(), string(content))
					if err != nil {
						util.Error("err: ", err)
						//log.Fatal("err: ", err)
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
						newFile, err := os.OpenFile(BasePath+"/"+newFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
						if err != nil {
							//log.Fatal("err: ", err)
							util.Error("err: ", err)
						}
						for ind := 0; ind < len(intermediate[i]); ind++ {
							fmt.Fprintf(newFile, "%+v %+v\n", intermediate[i][ind].Key, intermediate[i][ind].Value)
						}
						newFile.Close()
					}
					file.Close()
					SendRes(name, workerId, "", j) //Wait to deal
				} else if j.JobType == ReduceType {

					files, err := os.ReadDir(BasePath)
					if err != nil {
						//fmt.Printf("err: ", err)
						util.Error("err: ", err)
					}

					tempList := make([]*os.File, 0)

					for _, file := range files {
						if file.Name()[len(file.Name())-5:] == strconv.Itoa(j.JobId)+".txt" {
							tf, _ := os.Open(BasePath + "/" + file.Name())
							tempList = append(tempList, tf)
						}
					}
					tempKvList := make([][]KeyValue, 0)
					for _, v := range tempList {
						fileKvList := make([]KeyValue, 0)
						scanner := bufio.NewScanner(v)
						for scanner.Scan() {
							line := scanner.Text()
							t := strings.Split(line, " ")

							if len(t) > 1 {
								if len(t) != 2 {
									util.Error("Attention Now!")
									for _, tt := range t {
										util.Info(tt)
									}
									util.Error("Attention End!")
								}
								fileKvList = append(fileKvList, KeyValue{Key: t[0], Value: t[1]})
							}
							//v_int, _:= strconv.ParseInt(t[1], 10, 64)
						}
						tempKvList = append(tempKvList, fileKvList)
					}
					for _, v := range tempList {
						v.Close()
					}
					FinalList := Partition(tempKvList)
					//FinalReducePath+"/"+
					outputFile, err := os.OpenFile("mr-out-"+strconv.Itoa(j.JobId), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
					if err != nil {
						util.Error("Worker---Reduce---error: ", err)
					}
					util.Info("Worker---Reduce---output: ", outputFile.Name())
					i := 0
					for i < len(FinalList) {
						k := i + 1
						for k < len(FinalList) && FinalList[k].Key == FinalList[i].Key {
							k++
						}
						values := []string{}
						for k1 := i; k1 < k; k1++ {
							values = append(values, FinalList[k1].Value)
						}
						output := reducef(FinalList[i].Key, values)

						// this is the correct format for each line of Reduce output.
						fmt.Fprintf(outputFile, "%v %v\n", FinalList[i].Key, output)
						i = k
					}
					SendRes(name, workerId, outputFile.Name(), j)
					outputFile.Close()
				}
			}
		}
		time.Sleep(3 * time.Second)
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

	// send the RPC request, Wait for the reply.
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
	util.Info(reply.Head.Msg)
	if yes {
		if reply.Head.StatusCode == Ok {
			util.Success(reply.Head.Msg, "worker id: ", reply.WorkerId)
			workerId = reply.WorkerId
			return true
		} else if reply.Head.StatusCode == Mistake {
			util.Error(reply.Head.Msg)
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
	args.WorkerId = workerId
	args.WorkerName = name
	flag = 0
	yes := call("Coordinator.ApplyJob", &args, &reply)
	if yes {
		if reply.Head.StatusCode == Ok {
			j = reply.Task
			cor = true
			util.Success("worker %+v: %+v", name, reply.Head.Msg)
			return cor, j, flag
		} else if reply.Head.StatusCode == Nojob {
			j = nil
			cor = true
			util.Info("worker %+v: %+v", name, reply.Head.Msg)
			time.Sleep(time.Second)
			return cor, j, flag
		} else if reply.Head.StatusCode == Exit {
			return true, nil, 1
		} else if reply.Head.StatusCode == Mistake {
			util.Error("worker %+v: %+v", name, reply.Head.Msg)
			return false, nil, 0
		}
	} else {
		fmt.Printf("call applytask failed!\n")
	}
	return false, nil, 0
}

func SendRes(WorkName string, WorkerId int, outputfile string, Task *Job) bool {
	Task.JobStatus = Finish
	args := ResReq{
		Task:       Task,
		OutputName: outputfile,
		WorkerId:   WorkerId,
		WorkerName: WorkName,
	}
	reply := ResResp{}
	yes := call("Coordinator.RetRes", &args, &reply)
	if yes {
		if reply.Head.StatusCode == Ok {
			util.Success(reply.Head.Msg)
			return true
		} else if reply.Head.StatusCode == Mistake {
			util.Error(reply.Head.Msg)
			return false
		}
	} else {
		fmt.Printf("call applytask failed!\n")
	}
	return false
}

// send an RPC request to the coordinator, Wait for the response.
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
