package shardctrler


import (
	// "log"
	"sort"
	"time"
	"6.5840/raft"
	"6.5840/labrpc"
	"sync"
	"6.5840/labgob"
)

const (
	REQUEST_TIMEOUT				= 200
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs 			[]Config 					// indexed by config num
	lastApplied			int							// last applied command index
	opApplyChMap		map[int]chan Config			// commandIndex -> chan, Op apply channel for current request command, apply Op and reply after servers reach agreement
	lastRequestMap		map[int]int     			// Clientid -> LastRequestId, for every client, record last request it sent to detect duplicate
	groupShardsMap		map[int][]int				// gid -> shards, for every group, records shards assigned to find groups with min and max assigned shards
													// when need to balance the load, move shards from max to min		
}


type Op struct {
	// Your data here.
	OpType		string
	JoinArgs	JoinArgs
	LeaveArgs	LeaveArgs
	MoveArgs	MoveArgs
	QueryArgs	QueryArgs
	Config		Config
	ClientId	int
	RequestId	int
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	if sc.isDuplicateRequest(args.ClientId, args.RequestId) {
		reply.WrongLeader = false
		return
	}

	joinOp := Op {
		OpType: OpJoin,
		JoinArgs: *args,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, term, _ := sc.rf.Start(joinOp)
	opApplyCh := sc.getOpApplyCh(index)

	select {
	case <- opApplyCh:
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
	case <-time.After(REQUEST_TIMEOUT * time.Millisecond):
		reply.WrongLeader = true
	}

	go sc.deleteOpApplyCh(index)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	if sc.isDuplicateRequest(args.ClientId, args.RequestId) {
		reply.WrongLeader = false
		return
	}

	leaveOp := Op {
		OpType: OpLeave,
		LeaveArgs: *args,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, term, _ := sc.rf.Start(leaveOp)
	opApplyCh := sc.getOpApplyCh(index)

	select {
	case <- opApplyCh:
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
	case <-time.After(REQUEST_TIMEOUT * time.Millisecond):
		reply.WrongLeader = true
	}

	go sc.deleteOpApplyCh(index)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	if sc.isDuplicateRequest(args.ClientId, args.RequestId) {
		reply.WrongLeader = false
		return
	}

	moveOp := Op {
		OpType: OpMove,
		MoveArgs: *args,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, term, _ := sc.rf.Start(moveOp)
	opApplyCh := sc.getOpApplyCh(index)

	select {
	case <- opApplyCh:
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
		}
	case <-time.After(REQUEST_TIMEOUT * time.Millisecond):
		reply.WrongLeader = true
	}

	go sc.deleteOpApplyCh(index)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	queryOp := Op {
		OpType: OpQuery,
		QueryArgs: *args,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, term, _ := sc.rf.Start(queryOp)
	opApplyCh := sc.getOpApplyCh(index)

	select {
	case config := <- opApplyCh:
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Config = config
		}
	case <-time.After(REQUEST_TIMEOUT * time.Millisecond):
		reply.WrongLeader = true
	}

	go sc.deleteOpApplyCh(index)
}

func (sc *ShardCtrler) msgApplier() {
	for msg := range(sc.applyCh) {
		if msg.CommandValid {
			sc.applyCommandMsg(msg)
		}
	}
}

func (sc *ShardCtrler) applyCommandMsg(msg raft.ApplyMsg) {
	if msg.CommandIndex <= sc.lastApplied {
		return
	}
	sc.lastApplied = msg.CommandIndex

	op := msg.Command.(Op)
	var value Config
	if op.OpType != OpQuery && sc.isDuplicateRequest(op.ClientId, op.RequestId) {
		value = Config {}
	} else {
		value = sc.applyOp(op)
		sc.setLastRequest(op.ClientId, op.RequestId)
	}

	_, isLeader := sc.rf.GetState()
	if isLeader {
		opApplyCh := sc.getOpApplyCh(msg.CommandIndex)
		opApplyCh <- value
	}
}

func (sc *ShardCtrler) applyOp(op Op) Config {
	switch op.OpType {
	case OpJoin:
		sc.applyJoinOp(op.JoinArgs)
	case OpLeave:
		sc.applyLeaveOp(op.LeaveArgs)
	case OpMove:
		sc.applyMoveOp(op.MoveArgs)
	case OpQuery:
		return sc.applyQueryOp(op.QueryArgs)
	}
	return sc.configs[len(sc.configs) - 1]
}

func (sc *ShardCtrler) applyJoinOp(args JoinArgs) {
	// log.Printf("Join %v", args.Servers)
	newConfig := sc.getNewConfigFromLastConfig()
	for gid, servers := range(args.Servers) {
		newConfig.Groups[gid] = servers
		sc.groupShardsMap[gid] = make([]int, 0)
	}

	for {
		minGid, maxGid := sc.getGidWithMinAndMaxShards()
		min := len(sc.groupShardsMap[minGid])
		max := len(sc.groupShardsMap[maxGid])
		if max == 0 {
			// no groups have been assigned, init assignment then balance
			sc.initSharsAssignment(maxGid)
			continue
		}
		if max - min <= 1 {
			// already balanced
			break
		}
		// move one shard from max to min
		sc.groupShardsMap[minGid] = append(sc.groupShardsMap[minGid], sc.groupShardsMap[maxGid][0])
		sc.groupShardsMap[maxGid] = sc.groupShardsMap[maxGid][1:]
	}

	// log.Printf("After Join %v", sc.groupShardsMap)
	newConfig.Shards = sc.getNewShardsFromGroupShardsMap()
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyLeaveOp(args LeaveArgs) {
	newConfig := sc.getNewConfigFromLastConfig()
	freeShards := make([]int, 0);
	for _ , gid := range(args.GIDs) {
		delete(newConfig.Groups, gid)
		freeShards = append(freeShards, sc.groupShardsMap[gid]...)
		delete(sc.groupShardsMap, gid)
	}

	// If there are groups after leave
	if len(sc.groupShardsMap) > 0 {
		for _, shard := range(freeShards) {
			// move one shard from freeShards to min
			minGid, _ := sc.getGidWithMinAndMaxShards()
			sc.groupShardsMap[minGid] = append(sc.groupShardsMap[minGid], shard)
		}
	}

	newConfig.Shards = sc.getNewShardsFromGroupShardsMap()
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyMoveOp(args MoveArgs) {
	newConfig := sc.getNewConfigFromLastConfig()
	newConfig.Shards[args.Shard] = args.GID

	for gid, shards := range(sc.groupShardsMap) {
		for i, shard := range(shards) {
			if shard == args.Shard {
				sc.groupShardsMap[gid] = append(shards[0: i], shards[i + 1: len(shards)]...)
			}
		}
	}
	sc.groupShardsMap[args.GID] = append(sc.groupShardsMap[args.GID], args.Shard)

	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) applyQueryOp(args QueryArgs) Config {
	if args.Num == -1 {
		return sc.configs[len(sc.configs) - 1]
	}
	return sc.configs[args.Num]
}


// when need to append a new config, get a copy of last config and modify
func (sc *ShardCtrler) getNewConfigFromLastConfig() Config {
	lastConfig := sc.configs[len(sc.configs) - 1]
	return Config {
		Num:	len(sc.configs),
		Shards: deepCopyShards(lastConfig.Shards),
		Groups: deepCopyGroups(lastConfig.Groups),
	}
}

// find group with min and max assigned shards
func (sc *ShardCtrler) getGidWithMinAndMaxShards() (int, int) {
	// iterate map directly is not deterministic, sort map key first
	gids := make([]int, 0)
	for gid := range(sc.groupShardsMap) {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, maxGid := -1, -1
	min, max := NShards + 1, -1
	for _, gid := range(gids) {
		shardsNum := len(sc.groupShardsMap[gid])
		if shardsNum > max {
			maxGid = gid
			max = shardsNum
		}
		if shardsNum < min {
			minGid = gid
			min = shardsNum
		}
	}
	return minGid, maxGid
}

func (sc *ShardCtrler) getNewShardsFromGroupShardsMap() [NShards]int {
	var newShards [NShards] int
	for gid, shards := range(sc.groupShardsMap) {
		for _, shard := range(shards) {
			newShards[shard] = gid
		}
	}
	return newShards
}

// assign all shards to the given group
func (sc *ShardCtrler) initSharsAssignment(gid int) {
	for i := 0; i < NShards; i++ {
		sc.groupShardsMap[gid] = append(sc.groupShardsMap[gid], i)
	}
}

func deepCopyGroups(groups map[int][]string) map[int][]string {
	copyGroups := make(map[int][]string)
	for k, v := range(groups) {
		copyGroups[k] = v
	}
	return copyGroups
}

func deepCopyShards(shards [NShards]int) [NShards]int {
	var copyShards [NShards] int
	for i, v := range(shards) {
		copyShards[i] = v
	}
	return copyShards
}

func (sc *ShardCtrler) getOpApplyCh(index int) chan Config {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	opApplyCh, ok := sc.opApplyChMap[index]
	if ok {
		return opApplyCh
	}
	sc.opApplyChMap[index] = make(chan Config, 1)
	return sc.opApplyChMap[index]
}

func (sc *ShardCtrler) deleteOpApplyCh(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.opApplyChMap, index)
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int, requestId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastRequestId, ok := sc.lastRequestMap[clientId]
	if !ok {
		return false
	}
	return lastRequestId == requestId
}

func (sc *ShardCtrler) setLastRequest(clientId int, requestId int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.lastRequestMap[clientId] = requestId
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = 0
	sc.opApplyChMap = make(map[int]chan Config)
	sc.lastRequestMap = make(map[int]int)
	sc.groupShardsMap = make(map[int][]int)

	go sc.msgApplier()

	return sc
}
