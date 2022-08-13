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
	"fmt"
	"math/rand"
	"strings"
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
	log         *raftLog

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// Others:
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
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

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		old := rf.votedFor
		rf.votedFor = args.CandidateId
		DebugLog(dVote, "S%d T:%d, change vote from S%d to S%d T:%d",
			rf.me, rf.currentTerm, old, args.CandidateId, args.Term)
	}

	if args.Term == rf.currentTerm {
		// Figure 2, RequestVote RPC, Receive implementation:
		// Rule 2.
		mine := rf.log.lastLog()
		upToDate := args.LastLogTerm > mine.Term || (args.LastLogTerm == mine.Term && args.LastLogIndex >= mine.Index)
		if (rf.votedFor == InvalidId || rf.votedFor == args.CandidateId) && upToDate {
			rf.votedFor = args.CandidateId
			rf.resetRandomizedElectionTimeout() // necessary, or 2A warning term changed
			DebugLog(dVote, "S%d T:%d, vote for S%d T:%d",
				rf.me, rf.currentTerm, args.CandidateId, args.Term)
		} else {
			DebugLog(dVote, "S%d T:%d, already voted for S%d, reject S%d T:%d",
				rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
		}
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader {
		return rf.commitIndex, rf.currentTerm, false // todo -1?
	}
	index := rf.log.lastLog().Index
	term := rf.currentTerm
	ent := Entry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log.append(ent)
	rf.handleAppendEntries(false)
	DebugLog(dLog, "S%d start, T:%d I:%d Entry:%v", rf.me, term, index, ent)

	return index, term, true
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
			rf.handleAppendEntries(true)
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

	// 2B
	rf.log = newLog()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// 2B
	go rf.handleApply()

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

	voteCounter := 1
	completed := false
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	DebugLog(dInfo, "S%d create campaign, T:%d", rf.me, rf.currentTerm)

	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}

		go rf.askForVote(peer, &args, &voteCounter, &completed)
	}
}

func (rf *Raft) askForVote(to int, args *RequestVoteArgs, voteCounter *int, completed *bool) {
	DebugLog(dVote, "S%d asking for vote to S%d at T:%d", rf.me, to, args.Term)

	var reply RequestVoteReply
	ok := rf.sendRequestVote(to, args, &reply)
	if !ok {
		DebugLog(dError, "S%d send vote to S%d failed", rf.me, to)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugLog(dInfo, "S%d T:%d receive vote-reply from S%d T:%d",
		rf.me, args.Term, to, reply.Term)
	if !reply.VoteGranted {
		DebugLog(dVote, "S%d reject vote to S%d", to, rf.me)
		return
	}
	if reply.Term < args.Term {
		DebugLog(dVote, "S%d T:%d got stale vote from S%d T:%d",
			rf.me, args.Term, to, reply.Term)
		return
	}
	if reply.Term > args.Term {
		DebugLog(dVote, "S%d T:%d grant vote to S%d T:%d, update term",
			to, reply.Term, rf.me, args.Term)
		rf.updateTerm(reply.Term)
		return // todo return or not, how to wait every goroutine finish?
	}
	DebugLog(dVote, "S%d T:%d grant vote to S%d T:%d",
		to, reply.Term, rf.me, args.Term)

	*voteCounter++

	if *voteCounter <= len(rf.peers)/2 {
		DebugLog(dVote, "S%d got %d votes, not reach quorum yet", rf.me, voteCounter)
		return
	}
	if *completed {
		DebugLog(dVote, "S%d got %d votes, finish", rf.me, voteCounter)
		return
	}

	DebugLog(dVote, "S%d got quorum %d votes", rf.me, voteCounter)
	*completed = true

	if args.Term != rf.currentTerm || rf.state != StateCandidate {
		DebugLog(dWarn, "S%d T:[old:%d, now:%d] state:%d",
			rf.me, args.Term, rf.currentTerm, rf.state)
		return
	}

	rf.state = StateLeader
	DebugLog(dLeader, "S%d achieved majority for T%d(%d), convert to Leader(%d)",
		rf.me, rf.currentTerm, voteCounter, rf.state)

	// 2B
	// Figure 2, State, Volatile state on leaders
	// Reinitialized after election
	last := rf.log.lastLog().Index
	for i := range rf.peers {
		rf.nextIndex[i] = last + 1
		rf.matchIndex[i] = -1 // Students' Guide
	}
	rf.handleAppendEntries(true)
	DebugLog(dLeader, "S%d NI:%v", rf.me, rf.nextIndex)

}

// AppendEntriesArgs is from Figure 2
// 2B
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

func (a AppendEntriesArgs) String() string {
	var ents []string
	for _, e := range a.Entries {
		ents = append(ents, e.String())
	}
	entries := strings.Join(ents, ",")

	return fmt.Sprintf("AEArg[Leader:%d, T:%d, PLI:%d, PLT:%d, LCI:%d, Ents:%s]",
		a.LeaderId, a.Term, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, entries)
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// other fields for optimization
	Conflict bool
	XTerm    int
	XIndex   int
	XLen     int
}

func (rf *Raft) handleAppendEntries(isHeart bool) {
	//term := rf.currentTerm
	//args := RequestVoteArgs{ // todo use append entry args
	//	Term:        term,
	//	CandidateId: rf.me,
	//}
	failures := 0
	completed := true
	DebugLog(dInfo, "S%d Leader, state %d", rf.me, rf.state)

	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetRandomizedElectionTimeout() // todo check need or not?
			continue
		}

		// 2B
		// Rules for Servers, Leaders
		// Rule 3.
		last := rf.log.lastLog().Index
		next := rf.nextIndex[peer]
		if last >= next || isHeart {
			next = last

			prevLog := rf.log.at(next - 1)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]Entry, last-next+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.slice(next))

			go rf.tryAppendEntry(peer, &args, &failures, &completed)
		}

	}
}

func (rf *Raft) tryAppendEntry(to int, args *AppendEntriesArgs, failures *int, completed *bool) {
	var reply AppendEntriesReply

	rf.mu.Lock()
	defer rf.mu.Unlock()

	ok := rf.sendEntry(to, args, &reply)
	if !ok {
		*failures++
		if *failures <= len(rf.peers)/2 {
			DebugLog(dError, "S%d lost connections with %d peers, it's ok",
				rf.me, failures)
			return
		}
		if *completed {
			DebugLog(dError, "S%d lost connections with %d peers, already known",
				rf.me, failures)
			return
		}
		*completed = true
		rf.state = StateFollower
	}

	if reply.Term > rf.currentTerm {
		DebugLog(dLog2, "S%d T:%d recv AE reply with T:%d", rf.me, rf.currentTerm, reply.Term)
		rf.updateTerm(reply.Term)
		return
	}

	// todo, check args term?
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		next := match + 1                                 // todo may not safe, from Students' Guide
		rf.matchIndex[to] = max(rf.matchIndex[to], match) // increase monotonically
		rf.nextIndex[to] = max(rf.nextIndex[to], next)    // increase monotonically
		DebugLog(dLog2, "S%d T:%d AE success to S%d, MI:%d, NI:%d",
			rf.me, rf.currentTerm, to, rf.matchIndex[to], rf.nextIndex[to])
	} else if reply.Conflict {
		DebugLog(dLog2, "S%d T:%d AE failed to S%d, reply conflict",
			rf.me, rf.currentTerm, to)
		if reply.XTerm == -1 { // todo, correct?
			rf.nextIndex[to] = reply.XLen
		} else {
			li := rf.lastIndexMatchTerm(reply.XTerm)
			DebugLog(dLog2, "S%d T:%d AE failed to S%d, reply hint LI:%d",
				rf.me, rf.currentTerm, to, li)
			if li > 0 {
				rf.nextIndex[to] = li
			} else {
				rf.nextIndex[to] = reply.XIndex // todo, need investigate
			}
		}
		DebugLog(dLog2, "S%d T:%d update S%d NI:%d",
			rf.me, rf.currentTerm, to, rf.nextIndex[to])
	} // todo, need other condition?

	rf.leaderRules()
}

func (rf *Raft) lastIndexMatchTerm(t int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		term := rf.log.at(i).Term
		if term == t {
			return i
		} else if term < t {
			DebugLog(dLog2, "S%d T:%d can't find T:%d in log", rf.me, rf.currentTerm, t)
			break
		}
	}
	return -1
}

func (rf *Raft) leaderRules() {
	// Rules for Servers, Leaders
	// Rule 4:
	if rf.state != StateLeader {
		return
	}

	total := 0 // todo, count myself or not?

	for N := rf.commitIndex + 1; N <= rf.log.lastLog().Index; N++ {
		if rf.log.at(N).Term != rf.currentTerm {
			continue // shall log[N].term == currentTerm
		}

		for to := 0; to < len(rf.peers); to++ {
			if to == rf.me {
				continue // skip myself
			}

			if rf.matchIndex[to] >= N {
				total++
			}
		}
		if total > len(rf.peers)/2 {
			rf.commitIndex = N
			rf.triggerApply()
			DebugLog(dLog2, "S%d T:%d update CI:%d and trigger apply",
				rf.me, rf.currentTerm, rf.commitIndex)
			return
		}
	}
}

// todo use append entry args and reply
func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DebugLog(dInfo, "S%d T:%d receive AE:%s.", rf.me, rf.currentTerm, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false
	// AppendEntries RPC, Receiver implementation
	// Rule 1.
	if args.Term < rf.currentTerm {
		DebugLog(dDrop, "S%d T:%d drop AE:%s", rf.me, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		DebugLog(dLog2, "S%d T:%d only update term from AE", rf.me, rf.currentTerm)
		return // todo, why? need reset election timer?
	}

	rf.resetRandomizedElectionTimeout() // necessary, or 2A warning term changed

	// Rules for Servers, Candidate, Rule 3.
	if rf.state == StateCandidate {
		rf.state = StateFollower
	}

	// Rule 2.
	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		DebugLog(dLog2, "S%d T:%d AE conflict, reply(%+v)", rf.me, rf.currentTerm, reply)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.log.at(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			} else {
				DebugLog(dInfo, "S%d T:%d AE conflict, PrevLogIndex:%d, xIndex:%d, logTerm:%d, xTerm:%d",
					rf.me, rf.currentTerm, args.PrevLogIndex, xIndex, rf.log.at(xIndex-1).Term, xTerm) // todo, delete
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		DebugLog(dLog2, "S%d T:%d AE conflict, reply(%+v)", rf.me, rf.currentTerm, reply)
		return
	}

	for i, ent := range args.Entries {
		// Rule 3.
		if ent.Index <= rf.log.lastLog().Index && rf.log.at(ent.Index).Term != ent.Term {
			rf.log.truncate(ent.Index)
		}
		// Rule 4.
		if ent.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[i:]...)
			DebugLog(dLog2, "S%d T:%d AE(%v)", rf.me, rf.currentTerm, args.Entries[i:])
			break // todo, need or not?
		}
	}

	// Rule 5.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.triggerApply()
	}
	reply.Success = true

	DebugLog(dInfo, "S%d T:%d receive AE success, reply(%+v)", rf.me, rf.currentTerm, reply)
	return
}

func (rf *Raft) sendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) handleApply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// Figure 2, Rules for Servers, All Servers, #1
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			am := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DebugLog(dLog2, "S%d apply I:%d, applied cmds(%v)", rf.me, rf.lastApplied, rf.getAppliedCmds())
			rf.applyCh <- am // todo blocked?
			DebugLog(dLog2, "S%d apply msg sent to chan", rf.me)

		} else {
			rf.applyCond.Wait() // todo why?
			DebugLog(dLog2, "S%d wait apply cond", rf.me)
		}

	}

}

func (rf *Raft) triggerApply() {
	rf.applyCond.Broadcast()
	DebugLog(dLog2, "S%d T:%d trigger apply", rf.me, rf.currentTerm)
}

func (rf *Raft) getAppliedCmds() string {
	var cmds []string
	for i := 0; i <= rf.lastApplied; i++ {
		cmds = append(cmds, fmt.Sprintf("%4d", rf.log.at(i).Command))
	}
	return fmt.Sprintf(strings.Join(cmds, "|"))
}
