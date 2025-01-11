package mr

import "time"

type Job struct {
	JobName   string
	JobType   int
	JobStatus int       //任务目前状态
	StartTime time.Time //最后一次开始时间
	EndTime   time.Time //任务完成时间
	BelongID  int       //所属节点
}
