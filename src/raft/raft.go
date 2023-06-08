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
	"math/rand"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	// Raft role
	FOLLOWER					= 0
	CANDIDATE					= 1
	LEADER						= 2
	// Ticker
	HEARTBEAT_TICK 		= 150
	APPEND_TICK				= 15
	CHECK_TICK				= 20
	// RPC result code
	ERR 							= -1
	SUCCESS						= 1
	FAIL							= 0
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm			int
	votedFor				int
	log							[]LogEntry
	// Volatile state on all servers
	commitIndex			int
	lastApplied			int
	role						int
	electionTimeout	time.Time
	cond						*sync.Cond
	applyCh					chan ApplyMsg
	// Volatile state on leaders
	nextIndex				[]int
	matchIndex			[]int
}

type LogEntry struct {
	Command					interface{}
	Term						int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
		 d.Decode(&logs) != nil {
	  	DPrintf("cannot read persist")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
		rf.log = logs
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// set timeout for next election
func (rf *Raft) setElectionTimeout() {
	timeout := 500 + rand.Intn(500)
	rf.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(timeout))
}

// If RPC request or response contains term T > currentTerm, convert to follower
func (rf *Raft) convertToFollower(term int) {
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) convertToCandidate() {
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.setElectionTimeout()
	rf.persist()
}

func (rf *Raft) convertToLeader() {
	rf.role = LEADER
	for server := range(rf.peers) {
		if server == rf.me {
			continue
		}
		rf.nextIndex[server] = len(rf.log)
		rf.matchIndex[server] = 0
	}

	go rf.heartbeat()
	go rf.logAppendWorker()
	go rf.commitChecker()
}

func (rf *Raft) getLastLog() (int, int) {
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	return lastLogIndex, lastLogTerm
}

// check whether candiate's log is up-to-date, compare rules:
// If the logs have last entries with different terms, then the log with the later term is more up-to-date. 
// If the logs end with the same term, then whichever log is longer is more up-to-date.
func (rf *Raft) isCandidateLogUpToDate(args *RequestVoteArgs) bool {
	lastLogIndex, lastLogTerm := rf.getLastLog()

	if args.LastLogTerm < lastLogTerm {
		return false
	}
	if args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		return false
	}
	return true;
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term					int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term					int
	VoteGranted		bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isCandidateLogUpToDate(args) {
			// grand vote
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.setElectionTimeout()
			rf.persist()
			return
		}
	}

	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	Term						int
	LeaderId				int
	PrevLogIndex		int
	PrevLogTerm			int
	Entries 				[]LogEntry
	LeaderCommit		int
}

type AppendEntriesReply struct {
	Term						int
	Success					bool
	ConflictIndex		int
	ConflictTerm		int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	reply.Success = true
	rf.setElectionTimeout()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// 2.1 If a follower does not have prevLogIndex in its log
	// it should return with conflictIndex = len(log) and conflictTerm = None
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}
	// 2.2 If a follower does have prevLogIndex in its log, but the term does not match
	// it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i - 1].Term != conflictTerm {
				reply.ConflictIndex = i
			}
		}
		reply.ConflictTerm = conflictTerm
		reply.Success = false
		return
	}

	conflictOrNew := -1
	for i := 0; i < len(args.Entries); i++ {
		j := args.PrevLogIndex + i + 1
		if j >= len(rf.log) {
			// 4. Append any new entries not already in the log
			conflictOrNew = i
			break
		}
		if (rf.log[j].Term != args.Entries[i].Term) {
			// 3. If an existing entry conflicts with a new one, delete the existing entry and all that follow it
			conflictOrNew = i
			break
		}
	}
	if conflictOrNew != -1 {
		rf.log = append(rf.log[:conflictOrNew + args.PrevLogIndex + 1], args.Entries[conflictOrNew:]...)
		rf.persist()
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := int(math.Min(float64(args.LeaderCommit), float64(args.PrevLogIndex + len(args.Entries))))
		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.cond.Broadcast()
		}
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) int {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ERR
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
	// outdated(term confusion), drop the reply
	if rf.currentTerm != args.Term {
		return ERR;
	}
	if reply.VoteGranted {
		return SUCCESS
	}
	return FAIL
}

// result code for AppendEntries RPC:
// ERR: server error or outdated(term confusion) reply, drop the reply
// SUCCESS: append success, update next/match index
// FAIL: append fail, decrease nextIndex and retry append
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return ERR
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
	if rf.currentTerm != args.Term {
		return ERR
	}
	if reply.Success {
		return SUCCESS
	}
	return FAIL
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER || rf.killed() {
		return -1, -1, false
	}
	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry {
		Term: term,
		Command: command,
	})
	rf.persist()
	return index, term, true
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		role := rf.role
		electionTimeout := rf.electionTimeout
		rf.mu.Unlock()

		if role != LEADER && time.Now().Compare(electionTimeout) == 1 {
			rf.mu.Lock()
			rf.convertToCandidate()
			lastLogIndex, lastLogTerm := rf.getLastLog()
			// DPrintf("[Term %d]: Raft[%d] starts an election", rf.currentTerm, rf.me)
			args := RequestVoteArgs {
				Term: rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			}
			vote := 1
			rf.mu.Unlock()

			for server := range(rf.peers) {
				if server == rf.me {
					continue
				}

				go func(server int) {
					// NO LOCK HERE!!! Timeout request would block others
					// rf.mu.Lock()
					// defer rf.mu.Unlock()
					reply := RequestVoteReply {}
					res := rf.sendRequestVote(server, &args, &reply)
					// LOCK HERE INSTEAD!!!
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.role != CANDIDATE {
						return;
					}
					if res == SUCCESS {
						vote++
						if vote > len(rf.peers) / 2 {
							DPrintf("[Term %d]: Raft[%d] becomes leader", rf.currentTerm, rf.me)
							rf.convertToLeader()
						}
					}
				}(server)
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		me := rf.me
		role := rf.role
		currentTerm := rf.currentTerm
		commitIndex := rf.commitIndex
		lastLogIndex, lastLogTerm := rf.getLastLog()
		rf.mu.Unlock()

		if role != LEADER {
			return
		}

		for server := range(rf.peers) {
			if server == me {
				continue
			}

			args := AppendEntriesArgs {
				Term: currentTerm,
				LeaderId: me,
				Entries: make([]LogEntry, 0),
				PrevLogIndex: lastLogIndex,
				PrevLogTerm: lastLogTerm,
				LeaderCommit: commitIndex,
			}

			go func(server int) {
				reply := AppendEntriesReply {}
				rf.sendAppendEntries(server, &args, &reply)
			}(server)
		}

		time.Sleep(HEARTBEAT_TICK * time.Millisecond)
	}
}

func (rf *Raft) logAppendWorker() {
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		me := rf.me
		currentTerm := rf.currentTerm
		commitIndex := rf.commitIndex
		log := rf.log
		nextIndex := rf.nextIndex
		rf.mu.Unlock()
		
		if role != LEADER {
			return
		}

		for server := range(rf.peers) {
			if server == me {
				continue
			}

			go func(server int) {
				rf.mu.Lock()
				nextIndexForServer := nextIndex[server]
				// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
				if len(log) - 1 < nextIndexForServer {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs {
					Term: currentTerm,
					LeaderId: me,
					Entries: append([]LogEntry{}, log[nextIndexForServer:]...),
					PrevLogIndex: nextIndexForServer - 1,
					PrevLogTerm: log[nextIndexForServer - 1].Term,
					LeaderCommit: commitIndex,
				}
				reply := AppendEntriesReply {}
				DPrintf("[Term %d]: Raft[%d] append log[%d] to Raft[%d]", currentTerm, me, nextIndexForServer, server)
				rf.mu.Unlock()

				res := rf.sendAppendEntries(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				
				switch res {
					case ERR: 
						return
					case SUCCESS:
						// update nextIndex and matchIndex for follower
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[server] = rf.matchIndex[server] + 1
					case FAIL:
						// set nextIndex = conflictIndex by default
						rf.nextIndex[server] = reply.ConflictIndex
						// search its log for conflictTerm, set nextIndex to be the one beyond the index of the last entry in that term in its log
						conflictTerm := reply.ConflictTerm
						for i := args.PrevLogIndex; i > 0; i-- {
							if rf.log[i - 1].Term == conflictTerm {
								rf.nextIndex[server] = i;
								return
							}
						}
						DPrintf("[Term %d]: Raft[%d] need to retry append log[%d] to Raft[%d]", rf.currentTerm, rf.me, rf.nextIndex[server], server)
				}
			}(server)
		}

		time.Sleep(APPEND_TICK * time.Millisecond)
	}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: 
// set commitIndex = N
func (rf *Raft) commitChecker() {
	for rf.killed() == false {
		rf.mu.Lock()
		role := rf.role
		me := rf.me
		rf.mu.Unlock()
		
		if role != LEADER {
			return
		}

		isAgree := false
		rf.mu.Lock()
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			commit := 1
			for server := range(rf.peers) {
				if server == me {
					continue
				}
				if rf.matchIndex[server] >= N {
					commit++
				}
			}
			if commit > len(rf.peers) / 2 && rf.log[N].Term == rf.currentTerm {
				isAgree = true
				rf.commitIndex = N
				DPrintf("[Term %d]: Raft[%d] check commit[%d] success", rf.currentTerm, rf.me, N)
				break;
			}
		}
		rf.mu.Unlock()
		if isAgree {
			rf.cond.Broadcast()
		}

		time.Sleep(CHECK_TICK * time.Millisecond)
	}
}

// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
func (rf *Raft) commitApplier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.cond.Wait()
		}
		rf.lastApplied++
		applyMsg := ApplyMsg {
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command: rf.log[rf.lastApplied].Command,
		}
		DPrintf("[Term %d]: Raft[%d] apply commit[%d]", rf.currentTerm, rf.me, rf.lastApplied)
		rf.mu.Unlock()

		rf.applyCh <- applyMsg
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.role = FOLLOWER
	rf.setElectionTimeout()
	
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry { Term: 0 }
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commitApplier()

	return rf
}
