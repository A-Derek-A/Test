package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
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

type GetArgs struct {
	Key   string
	MsgId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type RaftReply struct {
	MgsId    int64
	ClientId int64
	Key      string
	Value    string
}
