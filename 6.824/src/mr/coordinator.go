package mr

import (
	util "6.824/utils"
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	fatal     = 1
	bad       = 2
	normal    = 3
	good      = 4
	excellent = 5
)

var (
	registerfail = errors.New("Can't register the worker because the name of worker has existed.")
)

type Coordinator struct {
	// Your definitions here.
	Filenames       []string      //所有文件名称
	Workers         []Slave       //已经注册过的Worker节点
	Verbose         bool          //是否开启日志
	TaskStage       int           //任务已经完成的的阶段
	MapTasks        []*Job        //Map任务数组
	MapTasksLeft    int           //Map任务尚未完成剩余量
	ReduceTasks     []*Job        //Reduce任务数组
	ReduceTasksLeft int           //Reduce任务尚未完成剩余量
	Glock           sync.Mutex    //分发任务时的锁
	Ginfo           StatisticInfo //Map Reduce过程全局信息统计
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Register(args *ConfigReq, reply *ConfigResp) error {
	woklen := len(c.Workers)

	for i := 0; i < woklen; i++ {
		if args.workerName == c.Workers[i].name {
			reply.head.err = registerfail
			reply.head.msg = "fail to register"
			reply.head.resptime = time.Now()
			reply.head.statusCode = 0
			reply.workerId = -1
			return nil
		}
	}

	c.Workers = append(c.Workers, Slave{
		id:       woklen,
		name:     args.workerName,
		level:    normal,
		lasttime: time.Now(),
		crash:    0,
		online:   true,
	})
	if c.Verbose {
		util.Info("Register a new worker")
	}

	reply.head = GeneralResp{
		err:        nil,
		msg:        "Success",
		resptime:   c.Workers[woklen].lasttime,
		statusCode: 1,
	}
	reply.workerId = c.Workers[woklen].id

	return nil
}

func (c *Coordinator) ApplyJob(args *JobReq, reply *JobResp) error {
	if c.TaskStage == 1 {

	} else if c.TaskStage == 2 {
		c.Glock.Lock()
		if c.ReduceTasksLeft == 0 {
			c.TaskStage = 3
		}
		for i := 0; i < len(c.ReduceTasks); i++ {
			if c.ReduceTasks[i].JobStatus == 0 || c.ReduceTasks[i].JobStatus == 2 {
				c.ReduceTasks[i].JobStatus = 1 // 1为正在运行 2为产生过错误 3为产生
				c.ReduceTasks[i].BelongID = args.workerId
				c.ReduceTasks[i].StartTime = time.Now()
				// wait to deal
			}
		}
	} else if c.TaskStage == 3 {
		c.Workers[args.workerId].online = false
		if c.Verbose {
			util.Info("worker id:%+v, name:%+v offline", args.workerId, args.workerName)
		}
		reply.head = GeneralResp{
			err:        nil,
			statusCode: 1,
			msg:        "shut down worker",
			resptime:   time.Now(),
		}
		reply.job = nil
	}
	return nil
}

func (c *Coordinator) RetRes(args *ResReq, reply *ResResp) error {
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
	ret := c.TaskStage == 4
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Filenames:       files,
		Workers:         make([]Slave, 0),
		Verbose:         true,
		TaskStage:       1,
		MapTasksLeft:    len(files),
		MapTasks:        make([]*Job, 0),
		ReduceTasks:     make([]*Job, 0),
		ReduceTasksLeft: 0,
		Ginfo:           StatisticInfo{0, 0},
	}
	//c.initMapTasks()

	// Your code here.

	c.server()
	return &c
}
