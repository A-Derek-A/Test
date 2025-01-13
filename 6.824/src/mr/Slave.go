package mr

import (
	"time"
)

const (
	Fatal     = 1
	Bad       = 2
	Normal    = 3
	Good      = 4
	Excellent = 5
)

const (
	On  = true
	Off = false
)

type Slave struct {
	Id       int       //节点的ID
	Name     string    //节点的名字
	Level    int       //节点的评级 对于任务完成的速度
	LastTime time.Time //上一次交互时间
	Crash    int       //发生故障的次数
	Online   bool      //是否在线
}
