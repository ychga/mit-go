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
	"6.5840/labgob"
	"bytes"
	"math/rand"
	"time"

	"sync"
	"sync/atomic"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg
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

type LogEntry struct {
	Index   int
	Command interface{}
	Term    int
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

type State int

const (
	Leader State = iota
	Follower
	Candidate
)

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A: leader election
	currentTerm int //Persistent
	votedFor    int //Persistent
	receivedRPC bool
	state       State

	// 2B: log
	log         []LogEntry //Persistent
	commitIndex int        //Initialized as lastIncludedTerm
	lastApplied int        //Initialized as indexOffset-1
	nextIndex   map[int]int
	matchIndex  map[int]int
	indexOffset int           //Persistent
	applyCh     chan ApplyMsg //Given

	//for cluster membership changes issues
	receivedRPCTime        time.Time
	minimumElectionTimeout int //Fixed

	//2D
	lastIncludedTerm int    //Persistent
	snapshot         []byte //Persistent
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.state == Leader
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil || e.Encode(rf.indexOffset) != nil || e.Encode(rf.lastIncludedTerm) != nil {
		return
	}
	raftstate := w.Bytes()
	if len(rf.snapshot) <= 0 {
		rf.persister.Save(raftstate, nil)
	} else {
		rf.persister.Save(raftstate, rf.snapshot)
	}

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var indexOffset int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&indexOffset) != nil || d.Decode(&lastIncludedTerm) != nil {
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.indexOffset = indexOffset
		rf.lastIncludedTerm = lastIncludedTerm
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if index > rf.commitIndex || index < rf.indexOffset {
		return
	}
	physicalIndex := rf.GetPhysicalIndex(index)
	rf.mu.Lock()
	rf.lastIncludedTerm = rf.log[physicalIndex].Term
	rf.log = rf.log[physicalIndex+1:]
	rf.indexOffset = index + 1
	rf.snapshot = snapshot
	rf.persist()
	rf.mu.Unlock()
	//log.Printf("%d snapshoted, now log is %v, offset is %d--------------------------------------", rf.me, rf.log, rf.indexOffset)
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
// handler function on the server side does not return. Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.HandleRequestVote", args, reply)
	return ok
}
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	/*
		The third issue is that removed servers (those not in Cnew) can
		disrupt the cluster.These servers will not receive heartbeats,
		so they will time out and start new elections.They will then send
		RequestVote RPCs with new term numbers, and this will cause the
		current leader to revert to follower state. A new leader will
		eventually be elected, but the removed servers will time out
		again and the process will repeat, resulting in poor availability.

		To prevent this problem, servers disregard RequestVote RPCs when
		they believe a current leader exists. Specifically, if a server
		receives a RequestVote RPC within the minimum election timeout
		of hearing from a current leader, it does not update its term or
		grant its vote. This does not affect normal elections, where each
		server waits at least a minimum election timeout before starting
		an election. However, it helps avoid disruptions from removed
		servers: if a leader is able to get heartbeats to its cluster, then
		it will not be deposed by larger term numbers.
	*/
	if time.Since(rf.receivedRPCTime).Milliseconds() < int64(rf.minimumElectionTimeout) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//log.Printf("%d believes there still be a leader, so it rejected %d's requestvote", rf.me, args.CandidateId)
		return
	}

	//log.Printf("%d is handling the requestvote from %d", rf.me, args.CandidateId)
	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	reply.VoteGranted = false
	// Reply false if term < currentTerm
	if args.Term >= rf.currentTerm {
		//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if args.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = args.Term
			rf.ConvertToFollower()
			rf.persist()
			//log.Printf("%d's votedFor is %d", rf.me, rf.votedFor)
			rf.mu.Unlock()
		}
		//If votedFor is -1 or candidateId
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			////log.Printf("%d : votedFor ok", rf.me)
			//and candidate’s log is at least as up-to-date as receiver’s log
			if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
				reply.VoteGranted = true
				rf.mu.Lock()
				rf.votedFor = args.CandidateId
				rf.receivedRPC = true
				rf.persist()
				//rf.grantedVote = true
				rf.mu.Unlock()
				////log.Printf("%d granted vote %d in term %d", rf.me, args.CandidateId, rf.currentTerm)
				////log.Printf("%d's lastLogTerm is %d, lastLogIndex is %d", rf.me, lastLogTerm, lastLogIndex)
				////log.Printf("%d's lastLogTerm is %d, lastLogIndex is %d", args.CandidateId, args.LastLogTerm, args.LastLogIndex)
			}
		}
		//log.Printf("%d's votedFor is %d", rf.me, rf.votedFor)
	}
	reply.Term = rf.currentTerm
}
func (rf *Raft) SendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
	return ok
}
func (rf *Raft) HandleAppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	rf.receivedRPC = true
	rf.receivedRPCTime = time.Now()
	lastLogIndex := rf.GetLastLogIndex()
	rf.mu.Unlock()
	////log.Printf("%d received AE from %d  with term %d while currentTerm is %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)

	if args.Term < rf.currentTerm {
		//Reply false if term < currentTerm, sender do not deserve my leader
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		//if RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		////log.Printf("%d got term %d  which is lower than leader %d's term %d", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.ConvertToFollower()
		rf.persist()
		rf.mu.Unlock()
	}

	////log.Printf("%d commitIndex is %d", rf.me, rf.commitIndex)

	//otherwise this is a real AppendEntriesRPC
	prevLogTerm := args.PrevLogTerm
	prevLogIndex := args.PrevLogIndex
	physicalIndex := rf.GetPhysicalIndex(prevLogIndex) //this could be -1 for the first Entry
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	/*
		场景1：Server没有任期6的任何Log，因此我们需要回退一整个任期的Log。
		S1: 4 5 5
		S2: 4 6 6 6
		场景2：S1收到了任期4的旧Leader的多条Log，但是作为新Leader，S2只收到了一条任期4的Log。所以这里，我们需要覆盖S1中有关旧Leader的一些Log。
		S1: 4 4 4
		S2: 4 6 6 6
		场景3：S1与S2的Log不冲突，但是S1缺失了部分S2中的Log。
		S1: 4
		S2: 4 6 6 6

		可以让Follower在回复Leader的AppendEntries消息中，携带3个额外的信息，来加速日志的恢复。这里的回复是指，Follower因为Log信息不匹配，拒绝了Leader的AppendEntries之后的回复。这里的三个信息是指：

		XTerm：这个是Follower中与Leader冲突的Log对应的任期号。在之前（7.1）有介绍Leader会在prevLogTerm中带上本地Log记录中，前一条Log的任期号。如果Follower在对应位置的任期号不匹配，它会拒绝Leader的AppendEntries消息，并将自己的任期号放在XTerm中。如果Follower在对应位置没有Log，那么这里会返回 -1。

		XIndex：这个是Follower中，对应任期号为XTerm的第一条Log条目的槽位号。

		XLen：如果Follower在对应位置没有Log，那么XTerm会返回-1，XLen表示空白的Log槽位数。

		场景1。Follower（S1）会返回XTerm=5，XIndex=2。Leader（S2）发现自己没有任期5的日志，它会将自己本地记录的，S1的nextIndex设置到XIndex，也就是S1中，任期5的第一条Log对应的槽位号。所以，如果Leader完全没有XTerm的任何Log，那么它应该回退到XIndex对应的位置（这样，Leader发出的下一条AppendEntries就可以一次覆盖S1中所有XTerm对应的Log）。

		场景2。Follower（S1）会返回XTerm=4，XIndex=1。Leader（S2）发现自己其实有任期4的日志，它会将自己本地记录的S1的nextIndex设置到本地在XTerm位置的Log条目后面，也就是槽位2。下一次Leader发出下一条AppendEntries时，就可以一次覆盖S1中槽位2和槽位3对应的Log。

		场景3。Follower（S1）会返回XTerm=-1，XLen=2。这表示S1中日志太短了，以至于在冲突的位置没有Log条目，Leader应该回退到Follower最后一条Log条目的下一条，也就是槽位2，并从这开始发送AppendEntries消息。槽位2可以从XLen中的数值计算得到。
	*/
	if lastLogIndex < prevLogIndex || (physicalIndex >= 0 && rf.log[physicalIndex].Term != prevLogTerm) {
		rf.mu.Lock()
		reply.Success = false
		reply.Term = rf.currentTerm
		if lastLogIndex < prevLogIndex {
			// case 3
			// 也可以直接返回lastLogIndex
			reply.XLen = prevLogIndex - lastLogIndex
			reply.XTerm = -1
		} else {
			reply.XTerm = rf.log[physicalIndex].Term
			for physicalIndex >= 0 && rf.log[physicalIndex].Term == reply.XTerm {
				physicalIndex--
			}
			//physicalIndex would be the preceding index of XTerm, so plus 1 before transfered into logIndex
			reply.XIndex = physicalIndex + 1 + rf.indexOffset
		}
		rf.mu.Unlock()
		//log.Printf("%d reject AE %v with term %d, log %v, reply is %v\n\n", rf.me, args, rf.currentTerm, rf.log, reply)
		return
	}

	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	//otherwise no need to overwrite it
	i := physicalIndex + 1
	j := 0
	rf.mu.Lock()
	for i < len(rf.log) && j < len(args.Entries) {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
		i++
		j++
	}
	// I doubt whether to truncate logs in heartbeat RPC
	rf.log = append(rf.log[:i], args.Entries[j:]...)
	rf.persist()
	rf.mu.Unlock()

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rf.mu.Lock()
	//lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit >= lastLogIndex {
			rf.commitIndex = lastLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	rf.mu.Unlock()
	reply.Success = true
	reply.Term = rf.currentTerm
	//log.Printf("%d received AE %v while currentTerm is %d, log is %v\n\n", rf.me, args, rf.currentTerm, rf.log)
}
func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.HandleInstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	rf.mu.Lock()
	physicalIndex := rf.GetPhysicalIndex(args.LastIncludedIndex + 1)
	if physicalIndex < 0 || physicalIndex >= len(rf.log) {
		rf.log = rf.log[:0]
	} else {
		rf.log = rf.log[physicalIndex:]
	}
	rf.lastApplied = args.LastIncludedIndex
	rf.indexOffset = args.LastIncludedIndex + 1
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.persist()

	rf.applyCh <- rf.GetApplyMsgForSnapshot()
	//apply the next command, need a later implement
	rf.mu.Unlock()
	//log.Printf("%d snapshoted, now log is %v, offset is %d--------------------------------------", rf.me, rf.log, rf.indexOffset)
}

// call this func during lock
func (rf *Raft) ConvertToLeader() {
	//log.Printf("%d convert into leader in term %d", rf.me, rf.currentTerm)
	rf.state = Leader
	lastLogIndex := rf.GetLastLogIndex()
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	//rf.DoRPC()
}

// call this func during lock
func (rf *Raft) ConvertToFollower() {
	if rf.state != Follower {
		rf.state = Follower
		//log.Printf("%d convert into follower in term %d", rf.me, rf.currentTerm)
	}
	rf.votedFor = -1
}

// call this func during lock
func (rf *Raft) ConvertToCandidate() {
	//log.Printf("%d convert into candidate in term %d", rf.me, rf.currentTerm)
	rf.state = Candidate
	rf.StartElection()
}

func (rf *Raft) GetPhysicalIndex(logIndex int) int {
	return logIndex - rf.indexOffset
}
func (rf *Raft) GetLastLogIndex() int {
	return len(rf.log) + rf.indexOffset - 1
}
func (rf *Raft) GetApplyMsg(logIndex int) ApplyMsg {
	i := rf.GetPhysicalIndex(logIndex)
	entry := rf.log[i]
	applyMsg := ApplyMsg{}
	applyMsg.CommandValid = true
	applyMsg.Command = entry.Command
	applyMsg.CommandIndex = logIndex
	//...
	return applyMsg
}
func (rf *Raft) GetApplyMsgForSnapshot() ApplyMsg {
	applyMsg := ApplyMsg{}
	applyMsg.SnapshotValid = true
	applyMsg.SnapshotIndex = rf.indexOffset - 1
	applyMsg.SnapshotTerm = rf.lastIncludedTerm
	applyMsg.Snapshot = rf.snapshot
	//...
	return applyMsg
}
func (rf *Raft) GetNewCommitIndex() int {
	//If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
	//and log[N].term == currentTerm:
	//set commitIndex = N
	left := rf.commitIndex
	right := rf.GetLastLogIndex()
	for left < right {
		logIndex := (left + right + 1) / 2
		votes := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= logIndex {
				votes += 1
			}
		}
		if votes <= len(rf.peers)/2 {
			right = logIndex - 1
		} else if rf.log[rf.GetPhysicalIndex(logIndex)].Term > rf.currentTerm {
			right = logIndex - 1
		} else if rf.log[rf.GetPhysicalIndex(logIndex)].Term < rf.currentTerm {
			left = logIndex
		} else {
			return logIndex
		}
	}
	return rf.commitIndex
}
func (rf *Raft) StartElection() {
	rf.mu.Lock()
	votes := 1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	//rf.grantedVote = true
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.GetLastLogIndex()
	args.LastLogTerm = rf.lastIncludedTerm
	if len(rf.log) > 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()

	//log.Printf("%d starts an Election in term %d", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := new(RequestVoteReply)
			result := rf.SendRequestVote(server, args, reply)
			if !result {
				return
			}
			if !reply.VoteGranted {
				if reply.Term > rf.currentTerm {
					//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.ConvertToFollower()
					rf.persist()
					rf.mu.Unlock()
				}
				return
			}

			rf.mu.Lock()
			votes++
			if votes <= len(rf.peers)/2 || rf.state == Leader {
				rf.mu.Unlock()
				return
			}
			rf.ConvertToLeader()
			rf.mu.Unlock()
			//log.Printf("%d is the leader in term %d", rf.me, rf.currentTerm)
			//Whether this code needs to be executed remains to be considered
			rf.DoRPC()
		}(i)
	}
}

// DoRPC do not call this func before unlocking
func (rf *Raft) DoRPC() {
	rf.mu.Lock()
	lastLogIndex := rf.GetLastLogIndex()
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if rf.state != Leader {
			break
		}
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			nextIndex := rf.nextIndex[server]
			rf.mu.Unlock()
			if rf.GetPhysicalIndex(nextIndex) < 0 {
				//have the leader send an InstallSnapshot RPC if it doesn't have the log entries required to bring a follower up to date.
				////log.Printf("SNAPSHOT: leader doesn't have the log entries required to bring %d up to date, expected Index: %d ", server, rf.nextIndex[server])
				rf.mu.Lock()
				args := new(InstallSnapshotArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.indexOffset - 1
				args.LastIncludedTerm = rf.lastIncludedTerm
				args.Data = rf.snapshot
				rf.mu.Unlock()
				reply := new(InstallSnapshotReply)
				result := rf.SendInstallSnapshot(server, args, reply)
				if !result {
					return
				}
				if reply.Term > rf.currentTerm {
					//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.ConvertToFollower()
					rf.persist()
					rf.mu.Unlock()
				} else {
					rf.nextIndex[server] = lastLogIndex + 1
				}
				return
			}

			//generate AppendEntries args
			rf.mu.Lock()
			args := new(AppendEntryArgs)
			args.Term = term
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			prevPhysicalIndex := rf.GetPhysicalIndex(args.PrevLogIndex)
			if prevPhysicalIndex >= 0 {
				args.PrevLogTerm = rf.log[prevPhysicalIndex].Term
			} else {
				args.PrevLogTerm = rf.lastIncludedTerm
			}

			//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			physicalIndex := rf.GetPhysicalIndex(rf.nextIndex[server])
			if physicalIndex < len(rf.log) {
				args.Entries = rf.log[physicalIndex:]
			}

			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			////log.Printf("leader %d is sending HB to %d in term %d, AE are %v", rf.me, server, rf.currentTerm, args)
			reply := new(AppendEntryReply)
			t0 := time.Now()
			result := rf.SendAppendEntries(server, args, reply)
			if !result || time.Since(t0).Milliseconds() > 40 {
				////log.Printf("leader %d 's AE to follower %d seems out of date", rf.me, server)
				return
			}
			if !reply.Success {
				if reply.Term > rf.currentTerm {
					//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.ConvertToFollower()
					rf.persist()
					rf.mu.Unlock()
				} else {
					//If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
					rf.mu.Lock()
					if reply.XTerm == -1 {
						rf.nextIndex[server] -= reply.XLen
					} else {
						i := rf.GetPhysicalIndex(args.PrevLogIndex)
						for i >= 0 && rf.log[i].Term > reply.XTerm {
							i--
						}
						if i < 0 || rf.log[i].Term < reply.XTerm {
							//case 1
							rf.nextIndex[server] = reply.XIndex
						} else {
							rf.nextIndex[server] = i + rf.indexOffset + 1
						}
					}
					//log.Printf("leader %d upated follower %d 's nextIndex to %d", rf.me, server, rf.nextIndex[server])
					rf.mu.Unlock()

				}
			} else {
				//If successful: update nextIndex and matchIndex for follower
				rf.mu.Lock()
				if rf.nextIndex[server] < lastLogIndex+1 {
					rf.nextIndex[server] = lastLogIndex + 1
				}
				if rf.matchIndex[server] < lastLogIndex {
					rf.matchIndex[server] = lastLogIndex
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}

// Start
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
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.mu.Unlock()
	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	//write this command into leader's log
	//log.Printf("-----------------------------------------AE-----------------------------------------")
	rf.mu.Lock()
	rf.log = append(rf.log, LogEntry{Index: rf.GetLastLogIndex() + 1, Term: rf.currentTerm, Command: command})
	rf.persist()
	lastLogIndex := rf.GetLastLogIndex()
	rf.mu.Unlock()
	//index = rf.DoAppendEntries(command)

	return lastLogIndex, term, isLeader
}

// Kill
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		////log.Printf("%d wake up in term %d", rf.me, rf.currentTerm)
		// Your code here (2A)
		if rf.state == Leader {
			ms := int64(40)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			////log.Printf("%d is the leader in term %d", rf.me, rf.currentTerm)
			//If command received from client: append entry to local log,
			//respond after entry applied to state machine (§5.3)
			//logIndex := len(rf.log)
			////log.Printf("before leader %d doing heartbeat", rf.me)
			rf.mu.Lock()
			rf.commitIndex = rf.GetNewCommitIndex()
			////log.Printf("leader %d caculated commitIndex: %d", rf.me, rf.commitIndex)
			rf.mu.Unlock()
			rf.DoRPC()
		} else {
			ms := 300 + (rand.Int63() % 500)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			if rf.state == Follower {
				if rf.receivedRPC {
					//A server remains in follower state as long as it receives valid RPCs from a leader or candidate.
					rf.mu.Lock()
					rf.receivedRPC = false
					//rf.votedFor = -1
					rf.mu.Unlock()
					////log.Printf("%d received RPC, still a follower", rf.me)
				} else {
					//election timeout, try election
					rf.ConvertToCandidate()
				}
			} else if rf.state == Candidate {
				//If election timeout elapses: start new election
				rf.StartElection()
			}
		}
	}
}
func (rf *Raft) allServersTicker() {
	for rf.killed() == false {
		// For all severs
		if rf.commitIndex > rf.lastApplied {
			rf.mu.Lock()
			rf.lastApplied++
			rf.applyCh <- rf.GetApplyMsg(rf.lastApplied)
			//log.Printf("%d applied an entry, index: %d", rf.me, rf.lastApplied)
			//apply the next command, need a later implement
			rf.mu.Unlock()
		}
		ms := int64(10)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.ConvertToFollower()
	rf.currentTerm = 0
	rf.indexOffset = 1
	rf.applyCh = applyCh
	rf.minimumElectionTimeout = 100
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.indexOffset - 1
	rf.commitIndex = rf.indexOffset - 1
	//log.Printf("make a raft sever, state is %v", rf)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.allServersTicker()
	return rf
}
