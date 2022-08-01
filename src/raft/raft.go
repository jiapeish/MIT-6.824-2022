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

// ApplyMsg is for:
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

// these are some const
const (
	InvalidId               = -1
	DefaultHeartbeatTimeout = 100 * time.Millisecond
)

// StateType represents the role of a node in a cluster.
type StateType uint64

// Possible values for StateType.
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader

	numStates
)

// Raft is for:
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// RSM states:
	state StateType
	//TickMs           uint
	heartbeatTimeout          time.Duration
	randomizedElectionTimeout time.Time
	//heartbeatElapsed int
	//electionElapsed  int

	// Figure 2: State
	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         raftLog

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// Others:

}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == StateLeader
	me := rf.me // for debug only
	rf.mu.Unlock()

	if isleader {
		DebugLog(dInfo, "S%d Leader, at T:%d", me, term)
	} else {
		DebugLog(dInfo, "S%d Follower: at T:%d", me, term)
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

// CondInstallSnapshot is for:
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot is for: the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs is for:
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// Figure 2: Request RPC:
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is for:
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// Figure 2: Request RPC:
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler example.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Figure 2, RequestVote RPC, Receive implementation:
	// Rule 1.
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DebugLog(dDrop, "S%d T:%d, reject stale request T:%d",
			rf.me, rf.currentTerm, args.Term)
		return
	}

	if args.Term == rf.currentTerm {
		if rf.votedFor == InvalidId {
			rf.votedFor = args.CandidateId
			rf.resetRandomizedElectionTimeout() // necessary, or 2A warning term changed
			DebugLog(dVote, "S%d T:%d, vote for S%d T:%d",
				rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			DebugLog(dVote, "S%d T:%d, already voted for S%d, reject S%d T:%d",
				rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
		}
	}

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		old := rf.votedFor
		rf.votedFor = args.CandidateId
		DebugLog(dVote, "S%d T:%d, change vote from S%d to S%d T:%d",
			rf.me, rf.currentTerm, old, args.CandidateId, args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = rf.votedFor == args.CandidateId
	DebugLog(dInfo, "S%d T:%d, request vote reply T:%d voted:%v",
		rf.me, rf.currentTerm, reply.Term, reply.VoteGranted)
}

func (rf *Raft) updateTerm(term int) {
	if term <= rf.currentTerm {
		return
	}

	old := rf.currentTerm
	rf.currentTerm = term
	rf.state = StateFollower
	rf.votedFor = InvalidId
	DebugLog(dTerm, "S%d update T:%d -> T:%d", rf.me, old, rf.currentTerm)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start is used for:
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill is used for:
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.heartbeatTimeout) // todo randomized?

		rf.mu.Lock()
		if rf.state == StateLeader {
			rf.appendEntry()
		}
		if time.Now().After(rf.randomizedElectionTimeout) {
			rf.campaign()
		}
		rf.mu.Unlock()

	}
}

// Make is used for:
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DebugLog(dInfo, "S%d created", me)
	rf.updateTerm(0)
	rf.heartbeatTimeout = DefaultHeartbeatTimeout
	rf.resetRandomizedElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) resetRandomizedElectionTimeout() {
	timeout := time.Duration(1000+rand.Intn(1000)) * time.Millisecond
	rf.randomizedElectionTimeout = time.Now().Add(timeout)
}

func (rf *Raft) campaign() {
	rf.currentTerm++
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.resetRandomizedElectionTimeout()
	term := rf.currentTerm
	voteCounter := 1
	completed := false
	DebugLog(dInfo, "S%d create campaign, T:%d", rf.me, term)

	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(to int) {
			DebugLog(dVote, "S%d asking for vote to S%d at T:%d", rf.me, to, term)
			args := RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(to, &args, &reply)
			if !ok {
				DebugLog(dError, "S%d send vote to S%d failed", rf.me, to)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			DebugLog(dInfo, "S%d T:%d receive vote-reply from S%d T:%d",
				rf.me, term, to, reply.Term)
			if !reply.VoteGranted {
				DebugLog(dVote, "S%d reject vote to S%d", to, rf.me)
				return
			}
			if reply.Term < term {
				DebugLog(dVote, "S%d T:%d got stale vote from S%d T:%d",
					rf.me, term, to, reply.Term)
				return
			}
			if reply.Term > term {
				DebugLog(dVote, "S%d T:%d grant vote to S%d T:%d, update term",
					to, reply.Term, rf.me, term)
				rf.updateTerm(reply.Term)
				return // todo return or not, how to wait every goroutine finish?
			}
			DebugLog(dVote, "S%d T:%d grant vote to S%d T:%d",
				to, reply.Term, rf.me, term)

			voteCounter++

			// todo correct?
			if completed || voteCounter <= len(rf.peers)/2 {
				DebugLog(dVote, "S%d got %d votes, finish", rf.me, voteCounter)
				return
			}
			DebugLog(dVote, "S%d got quorum %d votes", rf.me, voteCounter)
			completed = true
			if term != rf.currentTerm || rf.state != StateCandidate {
				DebugLog(dWarn, "S%d T:[old:%d, now:%d] state:%d",
					rf.me, term, rf.currentTerm, rf.state)
				return
			}

			rf.state = StateLeader
			DebugLog(dLeader, "S%d achieved majority for T%d(%d), convert to Leader(%d)",
				rf.me, rf.currentTerm, voteCounter, rf.state)
		}(peer)

	}
}

func (rf *Raft) appendEntry() {
	term := rf.currentTerm
	args := RequestVoteArgs{ // todo use entry args
		Term:        term,
		CandidateId: rf.me,
	}
	reply := RequestVoteReply{} // todo use entry reply
	failures := 1
	completed := true
	DebugLog(dInfo, "S%d Leader, state %d", rf.me, rf.state)

	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetRandomizedElectionTimeout() // todo check need or not?
			continue
		}

		go func(to int) {
			ok := rf.sendEntry(to, &args, &reply)
			if !ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				failures++
				if completed || failures <= len(rf.peers)/2 {
					DebugLog(dError, "S%d lost connections with %d peers",
						rf.me, failures)
					return
				}
				completed = true
				rf.state = StateFollower
			}
		}(peer)
	}
}

// todo use append entry args and reply
func (rf *Raft) AppendEntry(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DebugLog(dInfo, "S%d T:%d receive HBT from S%d T:%d",
		rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		DebugLog(dDrop, "S%d drop HBT from S%d", rf.me, args.CandidateId)
		return
	}

	rf.updateTerm(args.Term)
	rf.resetRandomizedElectionTimeout() // necessary, or 2A warning term changed
	DebugLog(dInfo, "S%d T:%d received HBT from S%d T:%d, term updated",
		rf.me, rf.currentTerm, args.CandidateId, args.Term)
}

func (rf *Raft) sendEntry(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
