package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu          sync.Mutex
	cache       map[string]string
	latestMsgId map[int]int64 // 仅用来存储
	latestMsg   map[int]Res   // 记录最新的的MsgId，消息的ID为一个纳秒级的时间戳
	//latestLog   map[int]Res   // 在get函数中记录最新get的latestMsgId的结果
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist := kv.cache[args.Key]
	if exist {
		reply.Value = value
		reply.Err = "Suc"
	} else {
		reply.Value = ""
		reply.Err = "Suc"
	}

	// val, exi := kv.latestMsg[args.ClientId]
	//fmt.Printf("val: %v, exs: %v, args.MsgId: %v\n", val, exi, args.MsgId)

	//if exi && val.MsgId > args.MsgId {
	//	reply.Err = "Time Exceeded."
	//	reply.Value = ""
	//} else if exi && val.MsgId == args.MsgId { //
	//	reply.Err = "Suc"
	//	reply.Value = kv.latestMsg[args.ClientId].Value
	//} else {
	//	//kv.latestMsg[args.ClientId] = args.MsgId
	//	value, exist := kv.cache[args.Key]
	//	if exist {
	//		reply.Value = value
	//		reply.Err = "Suc"
	//	} else {
	//		reply.Value = ""
	//		reply.Err = "Suc"
	//	}
	//	kv.latestMsg[args.ClientId] = Res{MsgId: args.MsgId, Type: "Get", Key: args.Key, Value: reply.Value}
	//	fmt.Printf("byte num: %v\n", unsafe.Sizeof(Res{Type: "Get", Key: args.Key, Value: reply.Value}))
	//}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.cache[args.Key] = args.Value
	reply.Value = args.Value
	reply.Err = "Suc"
	//val, exist := kv.latestMsg[args.ClientId]
	////fmt.Printf("val: %v, exs: %v, args.MsgId: %v\n", val, exist, args.MsgId)
	//if exist && val.MsgId > args.MsgId {
	//	reply.Err = "Time Exceeded."
	//	reply.Value = ""
	//} else if exist && val.MsgId == args.MsgId {
	//	reply.Err = "Repeat"
	//	reply.Value = kv.latestMsg[args.ClientId].Value
	//} else {
	//	//fmt.Println("------------------------------")
	//	//fmt.Printf("args.Value: %v\n", args.Value)
	//	//kv.latestMsg[args.ClientId] = args.MsgId
	//	kv.cache[args.Key] = args.Value
	//	reply.Value = args.Value
	//	reply.Err = "Suc"
	//	kv.latestMsg[args.ClientId] = Res{MsgId: args.MsgId, Type: "Put", Key: args.Key, Value: reply.Value}
	//}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, exs := kv.latestMsg[args.ClientId]
	// fmt.Printf("val: %v, exs: %v, args.MsgId: %v\n", val, exs, args.MsgId)
	if exs && val.MsgId > args.MsgId {
		reply.Err = "Time Exceeded."
		reply.Value = ""
	} else if exs && val.MsgId == args.MsgId {
		reply.Err = "Repeat"
		reply.Value = kv.latestMsg[args.ClientId].Value
	} else {
		//kv.latestMsg[args.ClientId] = args.MsgId
		value, exist := kv.cache[args.Key]
		if exist {
			kv.cache[args.Key] = value + args.Value
		} else {
			kv.cache[args.Key] = args.Value
		}
		reply.Err = "Suc"
		reply.Value = value
		kv.latestMsg[args.ClientId] = Res{MsgId: args.MsgId, Type: "Append", Key: args.Key, Value: reply.Value}
	}

}

//func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
//	// Your code here.
//}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.cache = make(map[string]string, 0)
	//kv.latestMsgId = make(map[int]int64, 0)
	kv.latestMsg = make(map[int]Res, 0)
	// You may need initialization code here.
	return kv
}
