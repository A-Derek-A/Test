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
	Normal  = 1 // 投票状态正常, 当前节点还没投过票
	Voted   = 2 // 在该Term内已经投过票
	Refuse  = 3 // 拉票方的Term号小于当前节点的Term号，因此拒绝投票
	Limited = 4 // 选举被限制
)

const ( // 节点持票状态
	Have = true  // 表明在该Term内还有票，没投
	Lose = false // 表明已经投票
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

	CurTerm        int           // 当前的任期号
	VoteState      bool          // 当前投票的状态
	Support        int           // 当前任期内支持谁
	Role           int           // 当前节点的角色
	Period         time.Duration // 倒计时
	Heart          time.Duration // 心跳包的发送时长
	Timer          *time.Ticker  // 计时器
	Logs           []Entry       // 用于存放log
	CommittedIndex int           // 已提交的日志索引号
	PeersInfo      []PInfo       // 用于存放对等节点的信息，包括 matchIndex，nextIndex
	// Point          int           // 当前指向logs最新的指针，
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Entry struct {
	Term    int         // 记录该Entry时的Term号
	Command interface{} // 该Entry包含的命令
}

type PInfo struct {
	MatchIndex int // Peer已经存入logs的Entry，每当一个节点成为Leader后将该值置为0
	NextIndex  int // Leader将要发给Follow的下一个索引，每当一个节点成为Leader后将该值置为Point + 1
	// CommitIndex   int  // Peer的CommitIndex
	// TrueNextIndex bool // Peer 的 NextIndex 是否正确，不正确一直发空包确认直到正确为止
	// NextIndex = MatchIndex + 1
}

// 需要将PrevIndex和PrevTerm发给Follower
// PrevIndex和PrevTerm 时leader 认为上一次发送的log的 Index 和其任期号，用于Follower节点在发现冲突时会退
// 需要Leader的CommitIndex
type AppendEntriesArgs struct {
	LeaderTerm  int   // Leader的Term号
	From        int   // 来自何方的ID号/心跳包时则是Leader的ID号
	To          int   // 去往何处的ID号
	PrevIndex   int   // Leader认为上一个发给Follower的索引
	PrevTerm    int   // Leader认为上一个发给Follower的Term号
	CommitIndex int   // 已经提交的索引
	Item        Entry // 具体的Entry
	// (2B)
}

// 需要将matchIndex发给Leader
type AppendEntriesReply struct {
	Term       int  // 其他节点的Term
	State      bool // 该条Append的状态
	MatchIndex int  // 回复节点的 Point 值

	// (2B)
}

// RoleChange 更改身份函数 包含降级和和升级操作,并更新任期
func (rf *Raft) RoleChange(target int, newTerm int) {
	rf.CurTerm = newTerm // 变成新任期
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
		for i := 0; i < len(rf.PeersInfo); i++ {
			rf.PeersInfo[i].NextIndex = len(rf.Logs) + 1 // 逻辑上下标1开始的Index，比实际的Logs中真实Index 大1
			rf.PeersInfo[i].MatchIndex = 0               // 逻辑上的MatchIndex
		}
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
// 需要新增选举限制，即对于Follower来说，如果他的log比候选人的log更新，那么他就不会给候选人投票
// log的最后一个条目，拥有更高的term号的更新，term号相同，则索引号更大的更新
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Me   int
	// 新增Candidate最后一条log的任期号和索引号
	LastLogIndex int // 最后一条日志的索引
	LastLogTerm  int // 最后一条日志的任期
	// 用于进行选举限制
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int //任期号
	BallotState int //返回投票信息
}

// CheckLog 目前仅在RequestVote里使用，所以不需要加锁
func (rf *Raft) CheckLog(argsTerm, argsLastLogIndex int) bool {
	// 需要新增索引检查在最开始时，Logs是长度为0的记录
	if len(rf.Logs) == 0 { // 如果目前没有Logs记录，那么直接返回true
		return true
	}
	if rf.Logs[len(rf.Logs)-1].Term > argsTerm {
		return false
	} else if rf.Logs[len(rf.Logs)-1].Term == argsTerm {
		if len(rf.Logs) > argsLastLogIndex {
			return false
		}
	}
	return true
	//if rf.Logs[rf.Point].Term > argsTerm {
	//	return false
	//} else if rf.Logs[rf.Point].Term == argsTerm {
	//	if rf.Point > argsLastLogIndex {
	//		return false
	//	}
	//}
	//return true
}

// example RequestVote RPC handler.
// 需要新增选举限制，即对于Follower来说，如果他的log比候选人的log更新，那么他就不会给候选人投票
// log的最后一个条目，拥有更高的term号的更新，term号相同，则索引号更大的更新
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Warning("requestVote> curTerm: %+v, argsTerm: %+v", rf.CurTerm, args.Term)
	//公共逻辑尽可能变少，Term
	switch {
	case args.Term < rf.CurTerm:
		reply.BallotState = Refuse
	case args.Term > rf.CurTerm: // 当前节点的Term 小于 发来的消息的节点，处理完
		rf.RoleChange(Follower, args.Term)
		fallthrough
	case args.Term == rf.CurTerm:
		if !rf.CheckLog(args.LastLogTerm, args.LastLogIndex) { // 没有通过选举人限制的检查
			reply.BallotState = Limited
		} else {
			if rf.VoteState == Have { // 是否已经投票
				rf.Support = args.Me
				reply.BallotState = Normal
				rf.VoteState = Lose
			} else if rf.VoteState == Lose {
				reply.BallotState = Voted
			}
		}
	}
	reply.Term = rf.CurTerm
	if reply.BallotState == Normal {
		rf.Info("reply.Term: %d, reply.BallotState: Normal", reply.Term)
	} else if reply.BallotState == Voted {
		rf.Info("reply.Term: %d, reply.BallotState: Voted", reply.Term)
	} else if reply.BallotState == Refuse {
		rf.Info("reply.Term: %d, reply.BallotState: Refuse", reply.Term)
	} else if reply.BallotState == Limited {
		rf.Info("reply.Term: %d, reply.BallotState: Limited", reply.Term)
	}
	//rf.Info("reply.Term: %d, ", )
	//rf.Warning("requestVote> curTerm: %+v, reply: %+v", rf.CurTerm, reply.Term)
	return

}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// (2B)

	// 1.发送心跳包不应被视为当前任期内的一条日志被提交，即心跳包不是非空指令
	// 2.只有收到集群上，一半的节点(>=len(peers)，因为它自己也算)当前任期内日志的AE回复，才能更改本节点的CommittedIndex，CommittedIndex为len(rf.Logs)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	flag := true // flag 作为局部变量，用于判断是否是刚切换为Follower身份

	switch {
	case args.LeaderTerm < rf.CurTerm: // 老的Leader发来的心跳
		reply.State = false // State语义变化，等待修改
	case args.LeaderTerm > rf.CurTerm: // 当前节点的信息陈旧，需要更新
		rf.RoleChange(Follower, args.LeaderTerm)
		rf.Support = args.From
		// reply.State = true						State 语义变化
		flag = false
		fallthrough
	case args.LeaderTerm == rf.CurTerm: // 现任Leader发来的心跳, 作为Follower需要重置定时器
		// CommittedIndex 只有在成为Leader时才有用
		// CommittedIndex 不需要判断，因为能选上Leader说明其CommittedIndex在大多数节点里都是最新的
		// reply里的MatchIndex作为Follower节点来说可以等效为CommittedIndex
		rf.CommittedIndex = args.CommitIndex
		if flag { // flag变为false意味着刚刚转变为Follower
			rf.Timer.Reset(rf.Period)
		}
		if (args.PrevIndex == 0 && len(rf.Logs) == 0) || (args.PrevIndex <= len(rf.Logs) && args.PrevTerm == rf.Logs[args.PrevIndex-1].Term) {
			// PrevIndex为0，并且该节点日志为空，说明没有日志
			// PrevIndex在本地日志中存在，并且能对上Term号
			// 有可能本地Log中的日志存在更靠后的Index，但是该条日志必然因为之前的宕机而保存失败，因为大部分节点都不存在该条日志而选举其他节点成功
			// 如果以上检查能通过，说明当前发来的日志包里是正确的包

			// 日志的提交部分还没写
			if args.PrevIndex < len(rf.Logs) { // 如果 PrevIndex为日志中的一个Index，则后面的日志为无效日志
				rf.Logs = rf.Logs[:args.PrevIndex]
			}
			rf.Logs = append(rf.Logs, args.Item)
			reply.State = true
		} else {
			reply.State = false
		}
		reply.MatchIndex = len(rf.Logs) // 直接告知该节点的MatchIndex
	}
	reply.Term = rf.CurTerm // 任期号统一修改
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
		rf.Error("rpc Call Error in sendRequestVote function : send to %+v, %+v", server, ok)
		return ok
	}

	// 一旦有人选举成功成为Leader，它应该将所有的其他人的NextIndex设置为自己的Point + 1，MatchIndex置为0
	// 并立即发送心跳包，该心跳包的entry为空，但应该prev相关里附上自己Point所指的entry的索引和Term号
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
	} else if reply.BallotState == Limited {

	}
	rf.Info("rpc Vote: node id: %+v, node term: %+v, node ballotState: %v", server, reply.Term, reply.BallotState)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, num *int) bool {

	// (2B)
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	if !ok {
		rf.Error("rpc Call Error in sendAppendEntries function : send to %+v, %+v", server, ok)
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1.需要修改，因为即使reply.State == false，也不一定是需要降级，因为可能是Follower刚重启，他的NextIndex还对不上
	// 2.应该在收到的任期号大于时立即降级为Follower
	// 3.reply.State为false，应该认为对方的日志冲突，我们应该不断减少NextIndex的发送，直到它发来一个true，然后将对方的MatchIndex从0调整到NextIndex - 1
	// 4.在收到reply后统一在ticker的心跳结束后再发送新日志/还是为每一个Follower设置一个Ticker？ 1

	if reply.Term < rf.CurTerm {
		rf.Info("your reply is outdated, because reply.Term: %d, but current Term: %d", reply.Term, rf.CurTerm)
	} else if reply.Term > rf.CurTerm { // 降为Follower就不需要维护peers的相关信息
		rf.RoleChange(Follower, reply.Term)
		// reply.MatchIndex
		return false
	} else if reply.Term == rf.CurTerm {
		if reply.State == false {

		} else if reply.State == true { // 刚刚发过去的包就是Follower需要，但仍需要鉴别是心跳包还是指令包以更新CommittedIndex
			emptyEntry := Entry{}
			if args.Item != emptyEntry && args.Item.Term == rf.CurTerm { // 不等于空包并且为当前任期，对更新CommittedIndex贡献+1
				*num++
				if *num > len(rf.peers)/2 {
					rf.CommittedIndex = len(rf.Logs)
					*num = 0
					// num置为0防止后续的AE回复多次设置CommittedIndex
					// num为函数内局部变量，可以有效的防止因并发而将后续的AE回复当成
				}
			} else if args.Item != emptyEntry && args.Item.Term != rf.CurTerm { // 说明这些包是已有的旧包，并且他们需要同步给其他的节点

			} else if args.Item == emptyEntry { // 当前包为空的心跳包

			}
		}
		rf.PeersInfo[server].MatchIndex = reply.MatchIndex
		rf.PeersInfo[server].NextIndex = rf.PeersInfo[server].MatchIndex + 1
	}

	//if reply.State == true {
	//	return true
	//} else if reply.State == false {
	//	rf.RoleChange(Follower, reply.Term)
	//	return false
	//}
	return ok
}

func (rf *Raft) sendAllHeartbeat() {
	if rf.Role != Leader {
		rf.Warning("only a leader could sendheartbeat")
	}
	tempNum := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 查看Leader节点日志进度，如果已经出现Peer所需要的NextIndex那么在Item里填装好
		tempEntry := Entry{}
		if rf.PeersInfo[i].NextIndex <= len(rf.Logs) {
			tempEntry = rf.Logs[rf.PeersInfo[i].NextIndex-1]
		}
		args := AppendEntriesArgs{
			From:        rf.me,
			To:          i,
			LeaderTerm:  rf.CurTerm,
			Item:        tempEntry,
			PrevIndex:   rf.PeersInfo[i].MatchIndex,
			PrevTerm:    rf.Logs[rf.PeersInfo[i].MatchIndex-1].Term,
			CommitIndex: rf.CommittedIndex,
		}
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &reply, &tempNum)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := true

	if rf.Role != Leader {
		return -1, -1, false
	}
	rf.Logs = append(rf.Logs, Entry{
		Term:    rf.CurTerm,
		Command: command,
	})
	index = len(rf.Logs)
	term = rf.CurTerm
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

// 初始化2B 的内容

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		CurTerm:   0,
		Support:   -1,
		VoteState: Have,
		Role:      Follower,
		Heart:     50 * time.Millisecond,
		Period:    time.Duration(500+rand.Intn(200)) * time.Millisecond,
		Logs:      make([]Entry, 0),
		PeersInfo: make([]PInfo, 0),
		//Point:          -1,	// Point代表了已经存入的指针位置，初始为-1，代表尚未有任何日志存入
		CommittedIndex: 0,
	}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Timer = time.NewTicker(rf.Period)
	// 根据传进来的peers初始化PeersInfo
	for i := 0; i < len(rf.peers); i++ {
		rf.PeersInfo = append(rf.PeersInfo, PInfo{
			MatchIndex: 0,
			NextIndex:  1,
		})
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
