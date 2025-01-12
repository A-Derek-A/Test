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

var (
	registerfail = errors.New("can't register the worker because the name of worker has existed")
)

const (
	MapS    = 1
	ReduceS = 2
	NotifyS = 3
	CloseS  = 4
)

type Coordinator struct {
	// Your definitions here.
	PartitionNum    int           //分区数量
	Filenames       []string      //所有文件名称
	Intermediates   []string      //中间文件名称
	Workers         []Slave       //已经注册过的Worker节点
	Verbose         bool          //是否开启日志
	TaskStage       int           //任务所处的阶段
	MapTasks        []*Job        //Map任务数组
	MapTasksLeft    int           //Map任务尚未完成剩余量
	ReduceTasks     []*Job        //Reduce任务数组
	ReduceTasksLeft int           //Reduce任务尚未完成剩余量
	Glock           sync.Mutex    //分发任务时的锁
	Wlock           sync.Mutex    //注册节点锁
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
	c.Wlock.Lock()
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
	c.Wlock.Unlock()

	if c.Verbose {
		util.Info("Register a new worker")
	}

	reply.head = GeneralResp{
		err:        nil,
		msg:        "Success",
		resptime:   c.Workers[woklen].lasttime,
		statusCode: ok,
	}
	reply.workerId = c.Workers[woklen].id

	return nil
}

func (c *Coordinator) ApplyJob(args *JobReq, reply *JobResp) error {
	if c.TaskStage == MapS {
		c.Glock.Lock()
		for i := 0; i < len(c.MapTasks); i++ {
			if c.ReduceTasks[i].JobStatus == wait || c.ReduceTasks[i].JobStatus == crash {
				c.MapTasks[i].JobStatus = in_progress  //更改任务运行状态
				c.MapTasks[i].BelongID = args.workerId //更改任务运行
				c.MapTasks[i].StartTime = time.Now()
				reply.job = c.MapTasks[i] //分发任务给job
				reply.head = GeneralResp{
					err:        nil,
					statusCode: ok,
					msg:        MapOk,
					resptime:   time.Now(),
				}
				c.Glock.Unlock()
				return nil
			}
		}
		reply.job = nil
		reply.head = GeneralResp{
			err:        nil,
			statusCode: nojob,
			msg:        Notask,
			resptime:   time.Now(),
		}
		c.Glock.Unlock()
		return nil
	} else if c.TaskStage == ReduceS {
		c.Glock.Lock()
		for i := 0; i < len(c.ReduceTasks); i++ {
			if c.ReduceTasks[i].JobStatus == wait || c.ReduceTasks[i].JobStatus == crash {
				c.ReduceTasks[i].JobStatus = in_progress // 1为正在运行 2为产生过错误 3为产生
				c.ReduceTasks[i].BelongID = args.workerId
				c.ReduceTasks[i].StartTime = time.Now()
				reply.job = c.ReduceTasks[i]
				reply.head = GeneralResp{
					err:        nil,
					statusCode: ok,
					msg:        ReduceOk,
					resptime:   time.Now(),
				}
				c.Glock.Unlock()
				return nil
			}
		}
		reply.job = nil
		reply.head = GeneralResp{
			err:        nil,
			statusCode: nojob,
			msg:        Notask,
			resptime:   time.Now(),
		}
		c.Glock.Unlock()
		return nil
	} else if c.TaskStage == NotifyS {
		c.Workers[args.workerId].online = off
		if c.Verbose {
			util.Info("worker id:%+v, name:%+v offline", args.workerId, args.workerName)
		}
		reply.head = GeneralResp{
			err:        nil,
			statusCode: exit,
			msg:        Shutdonw,
			resptime:   time.Now(),
		}
		reply.job = nil
	}
	return nil
}

func (c *Coordinator) RetRes(args *ResReq, reply *ResResp) error {

	c.Wlock.Lock()
	c.Workers[workerId].lasttime = time.Now()
	if args.jobstatus == false {
		c.Workers[workerId].crash++
	}
	c.Wlock.Unlock()
	if args.jobType == MapType {
		// wait to fix
		// 校验该任务当前所属的节点
		c.Glock.Lock()
		if args.jobstatus == true {
			c.MapTasks[args.jobId].JobStatus = finish
			c.MapTasks[args.jobId].EndTime = time.Now()
		} else {
			c.MapTasks[args.jobId].JobStatus = crash
			//
		}
		c.Glock.Unlock()
	} else if args.jobType == ReduceType {

	}
	return nil
}

func (c *Coordinator) loadMapTasks() {
	c.MapTasksLeft = len(c.Filenames)
	for i := 0; i < len(c.Filenames); i++ {
		c.MapTasks = append(c.MapTasks, &Job{
			ParNum:    c.PartitionNum,
			JobId:     i,
			JobStatus: wait,
			JobName:   c.Filenames[i],
			JobType:   MapType,
		})
	}
}

func (c *Coordinator) loadReduceTasks() {
	for i := 0; i < c.PartitionNum; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &Job{
			ParNum:    c.PartitionNum,
			JobId:     i,
			JobStatus: wait,
			JobName:   "reduce partition",
			JobType:   ReduceType,
		})
	}
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
	ret := c.TaskStage == CloseS
	// Your code here.
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		PartitionNum:    nReduce,
		Filenames:       files,
		Intermediates:   make([]string, 0),
		Workers:         make([]Slave, 0),
		Verbose:         true,
		TaskStage:       MapS,
		MapTasksLeft:    len(files),
		MapTasks:        make([]*Job, 0),
		ReduceTasks:     make([]*Job, 0),
		ReduceTasksLeft: nReduce,
		Ginfo:           StatisticInfo{0, 0},
	}
	c.loadMapTasks()

	// Your code here.
	go func(c *Coordinator) {
		for {
			if c.TaskStage == MapS {
				c.Glock.Lock()
				cnt := 0
				for i := 0; i < len(c.MapTasks); i++ {
					if c.MapTasks[i].JobStatus == finish {
						continue
					}
					cnt++
				}
				c.MapTasksLeft = cnt
				c.Glock.Unlock()
				if c.MapTasksLeft == 0 {
					c.loadReduceTasks()
					c.TaskStage = ReduceS
				}
			} else if c.TaskStage == ReduceS {
				c.Glock.Lock()
				cnt := 0
				for i := 0; i < len(c.ReduceTasks); i++ {
					if c.ReduceTasks[i].JobStatus == finish {
						continue
					}
					cnt++
				}
				c.ReduceTasksLeft = cnt
				c.Glock.Unlock()
				if c.ReduceTasksLeft == 0 {
					c.TaskStage = NotifyS
				}
			} else if c.TaskStage == NotifyS {
				c.Wlock.Lock()
				cnt := 0
				for i := 0; i < len(c.Workers); i++ {
					if c.Workers[i].online == on {
						cnt++
					}
				}
				c.Wlock.Unlock()
				if cnt == 0 {
					c.TaskStage = CloseS
					break
				}
			}
			time.Sleep(2 * time.Second)
		}
	}(&c)

	c.server()
	return &c
}
