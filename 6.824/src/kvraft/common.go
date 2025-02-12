package kvraft

const (
	// 需要经过Raft层给出的Err
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	// Server 可以直接判断的Err
	ErrOutDate = "ErrOutDate"
	Waiting    = "Waiting"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	MsgId    int64
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type RaftReply struct {
	MsgId int64
	Key   string
	Value string
	// Raft 信息
	Index int
	Term  int
	Err   Err
}

type GetArgs struct {
	Key      string
	MsgId    int64
	ClientId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
