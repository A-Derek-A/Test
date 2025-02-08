package kvsrv

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	MsgId    int64        // 消息ID
	MsgState bool         // MsgId对应的消息状态
	Timer    *time.Ticker // 对发出去的消息计时
	ClientId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.MsgId = 0
	ck.MsgState = true
	ck.ClientId = int(nrand())
	return ck
}

func GenerateMsgId() int64 {
	return time.Now().UnixMicro()
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	for {
		if ck.MsgState == true {
			ck.MsgId = GenerateMsgId()
			ck.MsgState = false
		}
		args := GetArgs{
			MsgId:    ck.MsgId,
			Key:      key,
			ClientId: ck.ClientId,
		}
		reply := GetReply{}

		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if !ok {
			//fmt.Printf("Error occurred in Get PRC\n")
		}
		//fmt.Printf("args: %v, reply: %v\n", args, reply)
		if reply.Err == "Suc" {
			ck.MsgState = true
			return reply.Value
		}
		time.Sleep(time.Millisecond * 50)
	}
	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	for {
		if ck.MsgState == true {
			ck.MsgId = GenerateMsgId()
			ck.MsgState = false
		}
		args := PutAppendArgs{
			ClientId: ck.ClientId,
			Op:       op,
			Key:      key,
			Value:    value,
			MsgId:    ck.MsgId, // 需要生成一个正确的ServerId
		}
		reply := PutAppendReply{}

		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if !ok {
			//fmt.Println("Error occured in the PutAppend RPC.")
		}
		//fmt.Printf("args: %v, reply: %v\n", args, reply)
		if reply.Err == "Suc" {
			//fmt.Println(reply.Value)
			ck.MsgState = true
			return reply.Value
		} else if reply.Err == "Repeat" {
			ck.MsgState = true
			return reply.Value
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
