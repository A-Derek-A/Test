package mr

import (
	"time"
)

const (
	fatal     = 1
	bad       = 2
	normal    = 3
	good      = 4
	excellent = 5
)

const (
	on  = true
	off = false
)

type Slave struct {
	id       int       //节点的ID
	name     string    //节点的名字
	level    int       //节点的评级 对于任务完成的速度
	lasttime time.Time //上一次交互时间
	crash    int       //发生故障的次数
	online   bool      //是否在线
}
