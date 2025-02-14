package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	util "6.824/utils"
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
	From     int // 该条命令来自哪一个服务器
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	MsgChan  map[int64]*chan RaftReply // 该管道是Raft给对应的ClientId的消息，server将该消息保存到LastMsg中
	LastMsg  map[int64]RaftReply       // LastMsg 保存最新的ClientId的消息
	RollBack map[int64]RaftReply       // 用于回退最新的LastMsg
	cache    map[string]string         // 存储具体键值的Map
	Timeout  time.Duration             // 超时Raft提交成功超时倒计时

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) SubmitToRaft(cmd Op) (rr RaftReply) {
	util.Info("Now in SubmitToRaft")
	ind, _, isLeader := kv.rf.Start(cmd) // 对于cmd，会有多个提交，但是只有一个会上传管道
	if !isLeader {
		util.Trace("kvserver %v is not leader.", kv.me)
		rr.Err = ErrWrongLeader
		rr.Value = ""
		return rr
	}
	util.Info("Start building channel, and command : %v", cmd)
	kv.mu.Lock()
	var t chan RaftReply
	if kv.MsgChan[int64(ind)] == nil { // 重复的ClientId的请求都被拦在了Get 或 PutAppend函数中，管道只会创建一次
		t = make(chan RaftReply, 1)
		kv.MsgChan[int64(ind)] = &t
	} else {
		rr.Err = Waiting
		return rr
	}
	kv.mu.Unlock()
	util.Info("SubmitToRaft start an ticker")

	select {
	case <-time.After(kv.Timeout):
		rr.Err = ErrWrongLeader
		util.Trace("kvserver %v is not leader select.", kv.me)
		rr.Key = cmd.Key
		rr.Value = cmd.Value
		rr.MsgId = cmd.MsgId
	case temp := <-t: // 一条Raft处理过，Server应用后的消息到了
		rr = temp
		util.Success("channel message >>> Err: %v, MsgId: %v, Key: %v, Value: %v, Index: %v, Term: %v", temp.Err, temp.MsgId, temp.Key, temp.Value, temp.Index, temp.Term)
	}

	util.Info("waiting a Lock")
	kv.mu.Lock()
	close(t)
	delete(kv.MsgChan, int64(ind))
	kv.mu.Unlock()
	return rr
}
func (kv *KVServer) RaftApplyServer() {
	// 在提交的Raft中，日志中会包括提交重复的内容。我们只能提交一遍
	// 对于重复的Client ID && 重复的Message ID
	// 网络分区后，由于之前的请求失败，会在不同的Leader上产生重复的请求，对于保存在Raft中，数据一致的只应用一条
	// 对于一个读取的 m ，我们可以查看MsgId，查看它是不是我们要的id，在处理完这个后，要的id+1，不符合Id，即小于等于的直接忽律
	// 应该怎么交付 ?
	for m := range kv.applyCh {
		util.Trace("kvserver: %v, m: %+v", kv.me, m)
		if m.CommandValid == false { // 不是Command
			// ignore other types of ApplyMsg
		} else { // Command命令
			var op Op
			op = m.Command.(Op)

			kv.mu.Lock()
			rr := RaftReply{
				MsgId: op.MsgId,
				Key:   op.Key,
				Value: op.Value,
				Err:   OK,
				Index: m.CommandIndex,
				Term:  m.SnapshotTerm,
			}
			val, ex := kv.LastMsg[op.ClientId]
			if (!ex && op.MsgId == 0) || val.MsgId+1 == op.MsgId { // 不存在并且该条记录MsgId为0， 或者是最新记录的下一条
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
				kv.LastMsg[op.ClientId] = rr
			} else if ex && val.MsgId >= op.MsgId { // 有最新一条记录
				rr = val
			} else {
				rr.Err = ErrOutDate
			}
			if op.From == kv.me && kv.MsgChan[int64(m.CommandIndex)] != nil { // op.From 是当时发命令的Leader 就是自己，即使现在可能不是，但因为已经提交了，所以命令肯定执行成功所以需要将消息返回给Channel
				// 但很显然这样没有考虑到，如果一个Server发生网络分区或者Crash，那么它就收的很可能都是以前的Cmd，在换Leader的情况下是可行的
				ch := kv.MsgChan[int64(m.CommandIndex)] // 即使Server已经不是Leader，对于它在Leader任期内已经处理并提交的请求，应该回复。
				*ch <- rr
			}
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
	Command := Op{
		Type:     "Get",
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		From:     kv.me,
	}
	Result := kv.SubmitToRaft(Command)
	reply.Err = Result.Err
	reply.Value = Result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 访问kv时需要加锁操作
	_, isLeader := kv.rf.GetState()
	if !isLeader { // 如果不是Leader，那么直接返回
		reply.Err = ErrWrongLeader
		return
	}
	Command := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		MsgId:    args.MsgId,
		From:     kv.me,
	}
	Result := kv.SubmitToRaft(Command)
	reply.Err = Result.Err
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

	kv.MsgChan = make(map[int64]*chan RaftReply)
	kv.LastMsg = make(map[int64]RaftReply)
	kv.cache = make(map[string]string)
	kv.RollBack = make(map[int64]RaftReply)
	kv.Timeout = 2 * time.Second
	go kv.RaftApplyServer()
	// You may need initialization code here.

	return kv
}
