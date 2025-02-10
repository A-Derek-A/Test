package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	ClientId int64 // 该命令来自哪个客户端
	MsgId    int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	MsgChan map[int64]chan RaftReply // 该管道是Raft给对应的ClientId的消息，server将该消息保存到LastMsg中
	LastMsg map[int64]RaftReply      // LastMsg 保存最新的ClientId的消息
	cache   map[string]string        // 存储具体键值的Map
	Timeout time.Duration            // 超时Raft提交成功超时倒计时

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) SubmitToRaft(cmd Op) (rr RaftReply) {
	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		rr.Err = ErrWrongLeader
		rr.Value = ""
		return rr
	}

	kv.mu.Lock()
	if kv.MsgChan[cmd.MsgId] == nil { // 重复的MsgId的请求都被拦在了Get 或 PutAppend函数中，管道只会创建一次
		kv.MsgChan[cmd.MsgId] = make(chan RaftReply, 1)
	}
	kv.mu.Unlock()
	ticker := time.NewTicker(kv.Timeout)

	select {
	case <-ticker.C:
		rr.Err = ErrWrongLeader
		rr.Key = cmd.Key
		rr.Value = cmd.Value
		rr.MsgId = cmd.MsgId
	case temp := <-kv.MsgChan[cmd.ClientId]: // 一条Raft处理过，Server应用后的消息到了
		if temp.Err == OK {

		}
	}

	kv.mu.Lock()
	close(kv.MsgChan[cmd.ClientId])
	delete(kv.MsgChan, cmd.ClientId)
	kv.mu.Unlock()
	return rr
}
func (kv *KVServer) RaftApplyServer() {
	for m := range kv.applyCh {
		if m.CommandValid == false { // 不是Command
			// ignore other types of ApplyMsg
		} else { // Command命令
			op := m.Command.(Op)
			kv.mu.Lock()
			rr := RaftReply{
				MsgId: op.MsgId,
				Key:   op.Key,
				Value: op.Value,
				Err:   OK,
			}
			if op.Type == "Get" {
				value, exist := kv.cache[op.Key]
				if exist {
					rr.Value = value
				} else {
					rr.Value = ""
					rr.Err = ErrNoKey
				}
			} else if op.Type == "Put" {
				kv.cache[op.Key] = op.Value
			} else if op.Type == "Append" {
				value, exist := kv.cache[op.Key]
				if exist {
					kv.cache[op.Key] = value + op.Value
				} else {
					kv.cache[op.Key] = op.Value
				}
			}

			kv.MsgChan[op.ClientId] <- kv.LastMsg[op.ClientId]
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) { // 首先得确认自己是否为Leader，如果后来发现不是Leader
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader { // 如果不是Leader，那么直接返回
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}
	kv.mu.Lock()
	val, exs := kv.LastMsg[args.ClientId]
	if exs && val.MsgId > args.MsgId {
		reply.Err = ErrOutDate
		reply.Value = ""
		kv.mu.Unlock()
	} else if exs && val.MsgId == args.MsgId {
		reply.Err = val.Err // 对于相等的有两种情况，一是Waiting，即已经在向Raft请求了，二是各种Error包括Ok
		reply.Value = val.Value
		kv.mu.Unlock()
	} else {
		Command := Op{
			Type:     "Get",
			Key:      args.Key,
			Value:    "",
			ClientId: args.ClientId,
			MsgId:    args.MsgId,
		}
		kv.LastMsg[args.ClientId] = RaftReply{ // 如果Raft认为自己是Leader节点，那么先
			MsgId: args.MsgId,
			Key:   args.Key,
			Value: "",
			Err:   Waiting,
		}
		kv.mu.Unlock()
		Result := kv.SubmitToRaft(Command)
		reply.Err = Result.Err
		reply.Value = Result.Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 访问kv时需要加锁操作
	kv.mu.Lock()
	val, exs := kv.LastMsg[args.ClientId]
	// fmt.Printf("val: %v, exs: %v, args.MsgId: %v\n", val, exs, args.MsgId)
	if exs && val.MsgId > args.MsgId { // 客户端存在，并且请求消息过时
		reply.Err = ErrOutDate
		kv.mu.Unlock()
	} else if exs && val.MsgId == args.MsgId { // 存在，消息Msg相等
		reply.Err = val.Err // 对于相等的有两种情况，一是Waiting，即已经在向Raft请求了，二是各种Error包括Ok
		kv.mu.Unlock()
	} else { // 不存在 或 请求消息更新
		Command := Op{
			Type:     args.Op,
			Key:      args.Key,
			Value:    args.Value,
			ClientId: args.ClientId,
			MsgId:    args.MsgId,
		}
		kv.LastMsg[args.ClientId] = RaftReply{
			MsgId: args.MsgId,
			Key:   args.Key,
			Value: args.Value,
			Err:   Waiting,
		}
		kv.mu.Unlock()
		Result := kv.SubmitToRaft(Command)
		reply.Err = Result.Err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.MsgChan = make(map[int64]chan RaftReply)
	kv.LastMsg = make(map[int64]RaftReply)
	kv.cache = make(map[string]string)
	kv.Timeout = 2 * time.Second
	// You may need initialization code here.

	return kv
}
