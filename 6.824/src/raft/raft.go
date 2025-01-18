package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower  = 1 // 追随者阶段
	Candidate = 2 // 候选人阶段
	Leader    = 3 // 领导者阶段
)

const ( // 当前应用在返回一票的信息状态上
	Normal = 1 // 投票状态正常, 当前节点还没投过票
	Voted  = 2 // 在该Term内已经投过票
	Refuse = 3 // 拉票方的Term号小于当前节点的Term号，因此拒绝投票
)

const ( // 节点持票状态
	Have = true  // 表明在该Term内还有票，没投
	Lose = false // 表明已经投票
)

const ()

const ( // 对于Leader节点，返回的Follower信息
	Out    = 1 // Follower认为的Leader节点信息
	Common = 2 // Follower 对于心跳包的回应
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	CurTerm   int           // 当前的任期号
	VoteState bool          // 当前投票的状态
	Support   int           // 当前任期内支持谁
	BallotNum int           // 选票的票数，仅在Candidate阶段使用
	Role      int           // 当前节点的角色
	Period    time.Duration // 倒计时
	Heart     time.Duration // 心跳包的发送时长
	Timer     *time.Ticker  // 计时器

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type AppendEntriesArgs struct {
	LeaderTerm int //Leader的Term号
	From       int //来自何方的ID号/心跳包时则是Leader的ID号
	To         int //去往何处的ID号
}

type AppendEntriesReply struct {
	Term  int  //其他节点的Term
	State bool //该条Append的状态
}

// RoleChange 更改身份函数 包含降级和和升级操作,并更新任期
func (rf *Raft) RoleChange(target int, newTerm int) {
	rf.CurTerm = newTerm
	switch target {
	case Follower:
		rf.Role = Follower        // 将该节点的角色重制为Follower
		rf.Timer.Reset(rf.Period) // 重置该节点的计时器为
		rf.Support = -1           // 重置该节点支持对象为无
		rf.VoteState = Have
	case Candidate:
		rf.Role = Candidate
		rf.Timer.Reset(rf.Period)
		rf.Support = rf.me
		rf.VoteState = Lose
	case Leader:
		rf.Role = Leader
		rf.Timer.Reset(rf.Heart)
		rf.Support = rf.me
		rf.VoteState = Lose
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.CurTerm
	if rf.Role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Me   int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int //任期号
	BallotState int //返回投票信息
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//公共逻辑尽可能变少，Term
	switch {
	case args.Term < rf.CurTerm:
		reply.BallotState = Refuse
	case args.Term > rf.CurTerm: // 当前节点的Term 小于 发来的消息的节点，处理完
		rf.RoleChange(Follower, args.Term)
		fallthrough
	case args.Term == rf.CurTerm:
		if rf.VoteState == Have {
			rf.Support = args.Me
			reply.BallotState = Normal
		} else if rf.VoteState == Lose {
			reply.BallotState = Voted
		}
	}
	reply.Term = rf.CurTerm
	return

}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.LeaderTerm < rf.CurTerm {
		reply.State = false
	} else if args.LeaderTerm == rf.CurTerm {
		reply.State = true
	} else if args.LeaderTerm > rf.CurTerm { // 当前节点的信息陈旧，需要更新
		rf.RoleChange(Follower, args.LeaderTerm)
		rf.Support = args.From
		reply.State = true
	}
	reply.Term = rf.CurTerm
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

//写一个节点打印函数

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, num *int) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		rf.Error("rpc Call Error in sendRequestVote function, %+v", ok)
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.BallotState == Normal {
		*num++
		if *num > (len(rf.peers) / 2) {
			if rf.Role == Leader { // 已经是Leader直接返回
				return true
			}
			rf.RoleChange(Leader, rf.CurTerm) // 不是则重置身份
			rf.sendAllHeartbeat()             // 立即向所有人发送一次心跳
			*num = 0
		}
	} else if reply.BallotState == Voted {

	} else if reply.BallotState == Refuse { // Refuse 和 Vote 本质是一样的，可以优化
		// 多个Refuse回应会多次重制为Follower
		rf.RoleChange(Follower, reply.Term)
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	if !ok {
		rf.Error("rpc Call Error in sendAppendEntries function, %+v", ok)
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.State == true {
		return true
	} else if reply.State == false {
		rf.RoleChange(Follower, reply.Term)
		return false
	}
	return ok
}

func (rf *Raft) sendAllHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs{
			From:       rf.me,
			To:         i,
			LeaderTerm: rf.CurTerm,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply)
	}
}

func (rf *Raft) sendAllVote() {
	BallotNum := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term: rf.CurTerm,
			Me:   rf.me,
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply, &BallotNum) //并发地发送选举请求
		//可能不知道哪个节点发来的投票结果
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		<-rf.Timer.C
		switch rf.Role {
		case Follower:
			rf.RoleChange(Candidate, rf.CurTerm)
			fallthrough
		case Candidate: // 只有Candidate才能将任期号+1
			//rf.Timer.Reset(rf.Period) // 开启竞选的倒计时
			rf.CurTerm++       // 任期号+1
			rf.Support = rf.me // 竞选支持者
			rf.sendAllVote()
		case Leader:
			rf.sendAllHeartbeat()
		}
	}
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		CurTerm: 0,
		Support: -1,
		Role:    Follower,
		Heart:   50 * time.Millisecond,
		Period:  time.Duration(350+rand.Intn(200)) * time.Millisecond,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Timer = time.NewTicker(rf.Period)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
