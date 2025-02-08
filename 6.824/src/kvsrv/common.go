package kvsrv

type Err string

// Put or Append
type PutAppendArgs struct {
	MsgId    int64
	Key      string
	Value    string
	Op       string
	ClientId int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err   Err
	Value string
}

type GetArgs struct {
	ClientId int
	MsgId    int64
	Key      string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type Res struct {
	MsgId int64
	Type  string
	Key   string
	Value string
}
