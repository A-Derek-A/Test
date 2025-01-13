package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	Ok      = 200 //代表任务成功
	Mistake = 300 //代表产生错误
	Nojob   = 400 //暂时没有工作，请等待
	Exit    = 500 //请退出程序

	MapOk    = "Apply Map Task Success."
	ReduceOk = "Apply Reduce Task Success."
	Notask   = "Please Wait for a new job"
	Shutdonw = "Please shut down."
	Success  = "Mission complete."
)

type GeneralResp struct {
	Err        error     //是否error
	StatusCode int       //调用状态码
	Msg        string    //回复信息
	Resptime   time.Time //消息时间
}

type JobResp struct {
	Head GeneralResp
	Task *Job
}

type ConfigResp struct {
	Head     GeneralResp
	WorkerId int
}

type ResResp struct {
	Head GeneralResp
}

type ConfigReq struct {
	WorkerName string
}

type JobReq struct {
	WorkerName string
	WorkerId   int
}

type ResReq struct {
	Task       *Job
	OutputName string
	WorkerId   int
	WorkerName string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
