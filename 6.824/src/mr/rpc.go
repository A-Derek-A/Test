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

type GeneralResp struct {
	err        error     //是否error
	statusCode int       //调用状态码
	msg        string    //回复信息
	resptime   time.Time //消息时间
}

type JobResp struct {
	head GeneralResp
	job  *Job
}

type ConfigResp struct {
	head     GeneralResp
	workerId int
}

type ResResp struct {
	head GeneralResp
	//
}

type ConfigReq struct {
	workerName string
}

type JobReq struct {
	workerName string
	workerId   int
}

type ResReq struct {
	//
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
