package kvraft

import (
	"6.824/labrpc"
	util "6.824/utils"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	MsgState  bool
	MsgId     int64
	ClientId  int64
	NowLeader int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.MsgId = 0
	ck.ClientId = nrand()
	// You'll have to add code here.
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	offset := 0
	for {
		if ck.MsgState == true {
			atomic.AddInt64(&ck.MsgId, 1)
			//ck.MsgId = GenerateMsgId()
			ck.MsgState = false
		}
		args := GetArgs{
			// MsgId:    ck.MsgId, message id 在GET时似乎没用
			// ClientId: ck.ClientId,
			Key:      key,
			MsgId:    ck.MsgId,
			ClientId: ck.ClientId,
		}
		reply := GetReply{}

		ok := ck.servers[(ck.NowLeader+offset)%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		if !ok { // 网络错误则使用
			util.Info("Error occurred in Get PRC")
			offset++
			if offset%len(ck.servers) == 0 {
				util.Info("PutAppend:The Cluster may be not available now, wait a second.")
				time.Sleep(time.Millisecond * 100)
				// util.Info("wake")
			} else {
				time.Sleep(time.Millisecond * 10)
			}
			// time.Sleep(time.Millisecond * 10)
			continue
		}
		//fmt.Printf("args: %v, reply: %v\n", args, reply)
		if reply.Err == ErrWrongLeader { // 回复并非Leader
			util.Info("Get:Wrong Leader %d, we expect next one.", (ck.NowLeader+offset)%len(ck.servers))
			offset++
			if offset%len(ck.servers) == 0 {
				util.Info("Get:The Cluster may be not available now, wait a second.")
				time.Sleep(time.Millisecond * 100)
			} else {
				time.Sleep(time.Millisecond * 10)
			}
			continue
		}
		if reply.Err == OK || reply.Err == ErrNoKey { // 有值成功和无值返回空都应认为成功
			ck.NowLeader = (ck.NowLeader + offset) % len(ck.servers) // offset 作为临时变量不置0
			ck.MsgState = true
			return reply.Value
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	offset := 0
	for {
		util.Info("begin")
		if ck.MsgState == true {
			atomic.AddInt64(&ck.MsgId, 1)
			//ck.MsgId = GenerateMsgId()
			ck.MsgState = false
		}
		args := PutAppendArgs{
			MsgId:    ck.MsgId,
			ClientId: ck.ClientId,
			Key:      key,
			Value:    value,
			Op:       op,
		}
		reply := PutAppendReply{}

		ok := ck.servers[(ck.NowLeader+offset)%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		if !ok { // 网络错误则使用
			util.Info("PutAppend:Error occurred in PutAppend PRC")
			offset++
			if offset%len(ck.servers) == 0 {
				util.Info("PutAppend:The Cluster may be not available now, wait a second.")
				time.Sleep(time.Millisecond * 10)
				// util.Info("wake")
			} else {
				time.Sleep(time.Millisecond * 10)
			}
			// time.Sleep(time.Millisecond * 10)
			continue
		}
		//fmt.Printf("args: %v, reply: %v\n", args, reply)
		if reply.Err == ErrWrongLeader { // 回复并非Leader
			util.Info("PutAppend:Wrong Leader %d, we expect next one.", (ck.NowLeader+offset)%len(ck.servers))
			offset++
			if offset%len(ck.servers) == 0 {
				util.Info("PutAppend:The Cluster may be not available now, wait a second.")
				time.Sleep(time.Millisecond * 500)
				// util.Info("wake")
			} else {
				time.Sleep(time.Millisecond * 10)
			}
			continue
		} else if reply.Err == OK || reply.Err == ErrNoKey { // 有值成功和无值返回空都应认为成功
			ck.NowLeader = (ck.NowLeader + offset) % len(ck.servers) // offset 作为临时变量不置0
			ck.MsgState = true
			return
		} else {
			util.Warning("reply: %v", reply)
		}

		time.Sleep(time.Millisecond * 10)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
