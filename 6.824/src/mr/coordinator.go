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
	FinalMerge      bool          //最终的合并
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
	defer c.Wlock.Unlock()
	woklen := len(c.Workers)
	for i := 0; i < woklen; i++ {
		if args.WorkerName == c.Workers[i].Name {
			reply.Head.Err = registerfail
			reply.Head.Msg = "fail to register"
			reply.Head.Resptime = time.Now()
			reply.Head.StatusCode = 0
			reply.WorkerId = -1
			return nil
		}
	}

	c.Workers = append(c.Workers, Slave{
		Id:       woklen,
		Name:     args.WorkerName,
		Level:    Normal,
		LastTime: time.Now(),
		Crash:    0,
		Online:   true,
	})

	util.Info("woklen : %+v", woklen)

	if c.Verbose {
		util.Info("Register a new worker")
	}

	reply.Head = GeneralResp{
		Err:        nil,
		Msg:        "Success",
		Resptime:   c.Workers[woklen].LastTime,
		StatusCode: Ok,
	}
	reply.WorkerId = c.Workers[woklen].Id

	return nil
}

func (c *Coordinator) ApplyJob(args *JobReq, reply *JobResp) error {
	c.Glock.Lock()
	defer c.Glock.Unlock()
	if c.TaskStage == MapS {
		util.Info("func Applyjob---map job number: ", len(c.ReduceTasks))
		for i := 0; i < len(c.MapTasks); i++ {
			if c.MapTasks[i].JobStatus == Wait || c.MapTasks[i].JobStatus == Crash {
				util.Info("func ApplyJob---task num: %d, task status", i, c.MapTasks[i].JobStatus)
				c.MapTasks[i].JobStatus = InProgress   //更改任务运行状态
				c.MapTasks[i].BelongID = args.WorkerId //更改任务运行
				c.MapTasks[i].StartTime = time.Now()
				reply.Task = c.MapTasks[i] //分发任务给job
				reply.Head = GeneralResp{
					Err:        nil,
					StatusCode: Ok,
					Msg:        MapOk,
					Resptime:   time.Now(),
				}

				return nil
			}
		}
		reply.Task = nil
		reply.Head = GeneralResp{
			Err:        nil,
			StatusCode: Nojob,
			Msg:        Notask,
			Resptime:   time.Now(),
		}

		return nil
	} else if c.TaskStage == ReduceS {

		for i := 0; i < len(c.ReduceTasks); i++ {
			if c.ReduceTasks[i].JobStatus == Wait || c.ReduceTasks[i].JobStatus == Crash {
				c.ReduceTasks[i].JobStatus = InProgress // 1为正在运行 2为产生过错误 3为产生
				c.ReduceTasks[i].BelongID = args.WorkerId
				c.ReduceTasks[i].StartTime = time.Now()
				reply.Task = c.ReduceTasks[i]
				reply.Head = GeneralResp{
					Err:        nil,
					StatusCode: Ok,
					Msg:        ReduceOk,
					Resptime:   time.Now(),
				}

				return nil
			}
		}
		reply.Task = nil
		reply.Head = GeneralResp{
			Err:        nil,
			StatusCode: Nojob,
			Msg:        Notask,
			Resptime:   time.Now(),
		}

		return nil
	} else if c.TaskStage == NotifyS {
		c.Workers[args.WorkerId].Online = Off
		if c.Verbose {
			util.Info("worker id:%+v, name:%+v offline", args.WorkerId, args.WorkerName)
		}
		reply.Head = GeneralResp{
			Err:        nil,
			StatusCode: Exit,
			Msg:        Shutdonw,
			Resptime:   time.Now(),
		}
		reply.Task = nil
	}
	return nil
}

func (c *Coordinator) RetRes(args *ResReq, reply *ResResp) error {

	c.Wlock.Lock()
	util.Info("worker: %d", args.WorkerId)
	c.Workers[workerId].LastTime = time.Now()
	if args.Task.JobStatus == Crash {
		c.Workers[workerId].Crash++
	}
	c.Wlock.Unlock()
	if args.Task.JobType == MapType {
		// Wait to fix
		// 校验该任务当前所属的节点
		util.Info("TaskReturn---work status: %d", args.Task.JobStatus)
		c.Glock.Lock()
		if args.Task.JobStatus == Finish {
			c.MapTasks[args.Task.JobId].JobStatus = Finish
			c.MapTasks[args.Task.JobId].EndTime = time.Now()
		} else {
			c.MapTasks[args.Task.JobId].JobStatus = Crash
			//
		}
		c.Glock.Unlock()
	} else if args.Task.JobType == ReduceType {
		c.Glock.Lock()
		if args.Task.JobStatus == Finish {
			c.ReduceTasks[args.Task.JobId].JobStatus = Finish
			c.ReduceTasks[args.Task.JobId].EndTime = time.Now()
		} else {
			c.ReduceTasks[args.Task.JobId].JobStatus = Crash
		}
		c.Glock.Unlock()
	}
	return nil
}

func (c *Coordinator) loadMapTasks() {
	c.MapTasksLeft = len(c.Filenames)
	for i := 0; i < len(c.Filenames); i++ {
		c.MapTasks = append(c.MapTasks, &Job{
			ParNum:    c.PartitionNum,
			JobId:     i,
			JobStatus: Wait,
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
			JobStatus: Wait,
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
	c.Glock.Lock()
	ret := c.TaskStage == CloseS
	util.Info("ret: ", ret)

	c.Glock.Unlock()
	// Your code here.
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		FinalMerge:      true,
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
			c.Glock.Lock()
			if c.TaskStage == MapS {
				cnt := 0
				for i := 0; i < len(c.MapTasks); i++ {
					if c.MapTasks[i].JobStatus == Finish {
						continue
					}
					cnt++
				}
				c.MapTasksLeft = cnt
				if c.MapTasksLeft == 0 {
					c.loadReduceTasks()
					c.TaskStage = ReduceS
				}
				//modified Lock
			} else if c.TaskStage == ReduceS {
				util.Info("We are in the Reduce Stage !!!!")
				cnt := 0
				for i := 0; i < len(c.ReduceTasks); i++ {
					if c.ReduceTasks[i].JobStatus == Finish {
						continue
					}
					cnt++
				}
				c.ReduceTasksLeft = cnt
				util.Info("go monitor func : Reduce Task Left: ", c.ReduceTasksLeft)
				if c.ReduceTasksLeft == 0 {
					c.TaskStage = NotifyS
				}
			} else if c.TaskStage == NotifyS {
				util.Info("We are in the Notify Stage !!!!")
				c.Wlock.Lock()
				cnt := 0
				for i := 0; i < len(c.Workers); i++ {
					if c.Workers[i].Online == On {
						cnt++
					}
				}
				c.Wlock.Unlock()
				util.Info("online worker number: ", cnt)
				if cnt == 0 && c.FinalMerge {
					c.TaskStage = CloseS
				}

			}
			c.Glock.Unlock()
			time.Sleep(2 * time.Second)
		}
	}(&c)

	//go func(c *Coordinator) {
	//	if c.TaskStage == NotifyS {
	//
	//
	//		//准备一个Reduce输出的列表
	//		tempList := make([]*os.File, 0)
	//
	//		//for _, file := range files {
	//		//	if file.Name()[len(file.Name())-5:] == strconv.Itoa(j.JobId)+".txt" {
	//		//		tf, _ := os.Open(BasePath + "/" + file.Name())
	//		//		tempList = append(tempList, tf)
	//		//	}
	//		//}
	//
	//		for i := 0; i < c.PartitionNum; i++{
	//			tf, err := os.Open(FinalReducePath + "/" + "mr-rd-" + strconv.Itoa(i) + ".txt")
	//			if err != nil{
	//				util.Error("file can't open.")
	//			}
	//			tempList = append(tempList, tf)
	//		}
	//
	//
	//		tempKvList := make([][]KeyValue, 0)
	//		for _, v := range tempList {
	//			fileKvList := make([]KeyValue, 0)
	//			scanner := bufio.NewScanner(v)
	//			for scanner.Scan() {
	//				line := scanner.Text()
	//				t := strings.Split(line, " ")
	//				if len(t) > 1{
	//					fileKvList = append(fileKvList, KeyValue{Key: t[0], Value: t[1]})
	//				}
	//				//v_int, _:= strconv.ParseInt(t[1], 10, 64)
	//			}
	//			tempKvList = append(tempKvList, fileKvList)
	//		}
	//		for _, v := range tempList {
	//			v.Close()
	//		}
	//		FinalList := Partition(tempKvList)
	//
	//		outputFile, err := os.OpenFile("mr-rd-"+strconv.Itoa(j.JobId)+".txt", os.O_WRONLY|os.O_CREATE, 0666)
	//		i := 0
	//		for i < len(FinalList) {
	//			k := i + 1
	//			for k < len(FinalList) && FinalList[k].Key == FinalList[i].Key {
	//				k++
	//			}
	//			values := []string{}
	//			for k1 := i; k1 < k; k1++ {
	//				values = append(values, FinalList[k1].Value)
	//			}
	//			output := reducef(FinalList[i].Key, values)
	//
	//			// this is the correct format for each line of Reduce output.
	//			fmt.Fprintf(outputFile, "%v %v\n", FinalList[i].Key, output)
	//			i = k
	//		}
	//	}
	//}(&c)

	c.server()
	return &c
}
