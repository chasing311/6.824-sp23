package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	REQUEST_TIMEOUT				= 200
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType					string
	Key							string
	Value						string
	ClientId				int
	RequestId				int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	KVStorage			map[string]string
	lastApplied			int							// last applied command index
	opApplyChMap		map[int]chan string			// commandIndex -> chan, Op apply channel for current request command, apply Op and reply after servers reach agreement
	lastRequestMap		map[int]int     			// Clientid -> LastRequestId, for every client, record last request it sent to detect duplicate
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	getOp := Op {
		OpType: OpGet,
		Key: args.Key,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}

	index, term, _ := kv.rf.Start(getOp)
	opApplyCh := kv.getOpApplyCh(index)

	select {
	case value := <- opApplyCh:
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err, reply.Value = OK, value
		}
	case <-time.After(REQUEST_TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go kv.deleteOpApplyCh(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.isDuplicateRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		return
	}

	putAppendOp := Op {
		OpType: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		RequestId: args.RequestId,
	}
	index, term, _ := kv.rf.Start(putAppendOp)
	opApplyCh := kv.getOpApplyCh(index)

	select {
	case <- opApplyCh:
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-time.After(REQUEST_TIMEOUT * time.Millisecond):
		reply.Err = ErrTimeout
	}

	go kv.deleteOpApplyCh(index)
}

func (kv *KVServer) msgApplier() {
	for msg := range(kv.applyCh) {
		if msg.CommandValid {
			kv.applyCommandMsg(msg)
		}
		if msg.SnapshotValid {
			kv.applySnapshotMsg(msg)
		}
	}
}

func (kv *KVServer) applyCommandMsg(msg raft.ApplyMsg) {
	if msg.CommandIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied = msg.CommandIndex

	op := msg.Command.(Op)
	var value string
	if op.OpType != OpGet && kv.isDuplicateRequest(op.ClientId, op.RequestId) {
		value = ""
	} else {
		value = kv.applyOp(op)
		kv.setLastRequest(op.ClientId, op.RequestId)
	}

	_, isLeader := kv.rf.GetState()
	if isLeader {
		opApplyCh := kv.getOpApplyCh(msg.CommandIndex)
		opApplyCh <- value
	}
	kv.snapshotIfNeed(msg.CommandIndex)
}

func (kv *KVServer) applySnapshotMsg(msg raft.ApplyMsg) {
	if msg.SnapshotIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied = msg.SnapshotIndex
	kv.switchToSnapshot(msg.Snapshot)
}

func (kv *KVServer) applyOp(op Op) string {
	switch op.OpType {
	case OpGet:
	case OpPut:
		kv.KVStorage[op.Key] = op.Value
	case OpAppend:
		kv.KVStorage[op.Key] += op.Value
	}
	return kv.KVStorage[op.Key]
}

func (kv *KVServer) getOpApplyCh(index int) chan string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	opApplyCh, ok := kv.opApplyChMap[index]
	if ok {
		return opApplyCh
	}
	kv.opApplyChMap[index] = make(chan string, 1)
	return kv.opApplyChMap[index]
}

func (kv *KVServer) deleteOpApplyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.opApplyChMap, index)
}

func (kv *KVServer) isDuplicateRequest(clientId int, requestId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestId, ok := kv.lastRequestMap[clientId]
	if !ok {
		return false
	}
	return lastRequestId == requestId
}

func (kv *KVServer) setLastRequest(clientId int, requestId int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastRequestMap[clientId] = requestId
}

func (kv *KVServer) snapshotIfNeed(snapshotIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1 {
		return
	}
	if kv.maxraftstate < kv.rf.GetRaftStateSize() {
		kv.rf.Snapshot(snapshotIndex, kv.encodeServerState())
	}
}

func (kv *KVServer) encodeServerState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KVStorage)
	e.Encode(kv.lastRequestMap)
	serverState := w.Bytes()
	return serverState
}

func (kv *KVServer) switchToSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var KVStorage map[string]string
	var lastRequestMap map[int]int
	if d.Decode(&KVStorage) != nil ||
	   d.Decode(&lastRequestMap) != nil {
	  	DPrintf("cannot read persist")
	} else {
	  kv.KVStorage = KVStorage
		kv.lastRequestMap = lastRequestMap
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.KVStorage = make(map[string]string)
	kv.lastApplied = 0
	kv.opApplyChMap = make(map[int]chan string)
	kv.lastRequestMap = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.switchToSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.msgApplier()

	return kv
}
