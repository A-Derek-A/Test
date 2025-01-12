package mr

import (
	"time"
)

const (
	wait        = 0
	in_progress = 1
	crash       = 2
	finish      = 3

	MapType    = 1
	ReduceType = 2
)

type Job struct {
	//静态
	JobName string //相关文件名
	JobType int    //任务类型
	//动态
	JobStatus int       //任务目前状态
	StartTime time.Time //最后一次开始时间
	EndTime   time.Time //任务完成时间
	BelongID  int       //所属节点
}
