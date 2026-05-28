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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state           int
	lastHeartbeat   time.Time
	electionTimeout time.Duration
	dead            int32
}

const (
	follower = iota
	candidate
	leader
)

const heartbeatInterval = 100 * time.Millisecond

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomElectionTimeout()
}

func (rf *Raft) lastLogIndexLocked() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTermLocked() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) isLogUpToDateLocked(index int, term int) bool {
	lastTerm := rf.lastLogTermLocked()
	if term != lastTerm {
		return term > lastTerm
	}
	return index >= rf.lastLogIndexLocked()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = []LogEntry{{Term: 0}}
		return
	}
	if len(rf.log) == 0 {
		rf.log = []LogEntry{{Term: 0}}
	}
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = follower
		rf.persist()
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.state = follower
		rf.resetElectionTimerLocked()
		rf.persist()
		reply.VoteGranted = true
	}
	reply.Term = rf.currentTerm
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.state = follower
	rf.resetElectionTimerLocked()

	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		reply.Term = rf.currentTerm
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		reply.Term = rf.currentTerm
		return
	}

	changed := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index < len(rf.log) {
			if rf.log[index].Term != entry.Term {
				rf.log = rf.log[:index]
				rf.log = append(rf.log, args.Entries[i:]...)
				changed = true
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			changed = true
			break
		}
	}
	if changed {
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		lastIndex := rf.lastLogIndexLocked()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = follower
	rf.resetElectionTimerLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = leader
	lastIndex := rf.lastLogIndexLocked()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
	}
	rf.matchIndex[rf.me] = lastIndex
	rf.resetElectionTimerLocked()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == leader {
		rf.mu.Unlock()
		return
	}
	rf.state = candidate
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	lastLogIndex := rf.lastLogIndexLocked()
	lastLogTerm := rf.lastLogTermLocked()
	rf.resetElectionTimerLocked()
	rf.persist()
	if int32(1) > int32(len(rf.peers)/2) {
		rf.becomeLeaderLocked()
		rf.mu.Unlock()
		go rf.broadcastHeartbeats()
		return
	}
	rf.mu.Unlock()

	votes := int32(1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			if !rf.sendRequestVote(server, args, &reply) || rf.killed() {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != candidate || rf.currentTerm != term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}
			if reply.VoteGranted && atomic.AddInt32(&votes, 1) > int32(len(rf.peers)/2) {
				rf.becomeLeaderLocked()
				go rf.broadcastHeartbeats()
			}
		}(i)
	}
}

func (rf *Raft) replicateOneRound(server int) {
	rf.mu.Lock()
	if rf.state != leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := append([]LogEntry(nil), rf.log[rf.nextIndex[server]:]...)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.sendAppendEntries(server, args, &reply) || rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader || rf.currentTerm != term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	if reply.Success {
		matchIndex := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = matchIndex
		rf.nextIndex[server] = matchIndex + 1
		rf.updateCommitIndexLocked()
		return
	}

	if reply.ConflictTerm != -1 {
		lastIndexOfTerm := -1
		for i := rf.lastLogIndexLocked(); i > 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				lastIndexOfTerm = i
				break
			}
		}
		if lastIndexOfTerm != -1 {
			rf.nextIndex[server] = lastIndexOfTerm + 1
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	} else {
		rf.nextIndex[server] = reply.ConflictIndex
	}
	if rf.nextIndex[server] < 1 {
		rf.nextIndex[server] = 1
	}
}

func (rf *Raft) broadcastHeartbeats() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateOneRound(i)
	}
}

func (rf *Raft) updateCommitIndexLocked() {
	for index := rf.lastLogIndexLocked(); index > rf.commitIndex; index-- {
		if rf.log[index].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= index {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = index
			return
		}
	}
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		timedOut := rf.state != leader && time.Since(rf.lastHeartbeat) >= rf.electionTimeout
		rf.mu.Unlock()
		if timedOut {
			rf.startElection()
		}
	}
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		time.Sleep(heartbeatInterval)
		rf.mu.Lock()
		isLeader := rf.state == leader
		rf.mu.Unlock()
		if isLeader {
			rf.broadcastHeartbeats()
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			index := rf.lastApplied
			command := rf.log[index].Command
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{Index: index, Command: command}
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.state == leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		index = rf.lastLogIndexLocked()
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
	}
	rf.mu.Unlock()

	if isLeader {
		go rf.broadcastHeartbeats()
	}

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

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
	rf.applyCh = applyCh

	// Your initialization code here.
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.state = follower
	rf.resetElectionTimerLocked()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTicker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}
