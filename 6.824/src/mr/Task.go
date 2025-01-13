package mr

import (
	"time"
)

const (
	Wait       = 0
	InProgress = 1
	Crash      = 2
	Finish     = 3

	MapType    = 1
	ReduceType = 2
)

type Job struct {
	//静态
	JobId   int    //Map阶段为任务编号，Reduce阶段为需要执行的分区号
	JobName string //相关文件名
	JobType int    //任务类型
	ParNum  int    //分区数量
	//动态
	JobStatus int       //任务目前状态
	StartTime time.Time //最后一次开始时间
	EndTime   time.Time //任务完成时间
	BelongID  int       //所属节点
}
