package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key       string
	Value     string
	RequestID int64
	Command   string
	ClerkID   int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	requestIDMap   map[int64]int64
	requestChanMap map[int]chan Op
	keyValueMap    map[string]string
	lastApplied    int
}

func (kv *KVServer) HandleAnyOperation(args *GetPutAppendArgs, reply *GetPutAppendReply) {
	if !kv.killed() {

		kv.mu.Lock()

		clerkRequestID := kv.requestIDMap[args.ClerkID]
		getOp := "Get"
		if args.Op != getOp && args.RequestID <= clerkRequestID {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}

		op := Op{
			args.Key,
			args.Value,
			args.RequestID,
			args.Op,
			args.ClerkID}
		index, _, leaderID := kv.rf.Start(op)

		if !leaderID {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}

		channel := kv.getChannelForIndex(index)

		kv.mu.Unlock()
		reply.Err = ErrTimeOut

		kv.handleResponseOrTimeout(channel, index, op, args, reply)

	} else {
		reply.Err = ErrWrongLeader
	}

}

func (kv *KVServer) handleResponseOrTimeout(channel chan Op, index int, op Op,
	args *GetPutAppendArgs, reply *GetPutAppendReply) {

	select {
	case <-time.After(50 * time.Millisecond):
		kv.handleTimeout(index, reply)
	case msg := <-channel:
		kv.handleGetResponse(msg, index, op, args, reply)
	}
}

func (kv *KVServer) handleTimeout(index int, reply *GetPutAppendReply) {
	reply.Err = ErrTimeOut
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.requestChanMap, index)
}

func (kv *KVServer) handleGetResponse(msg Op, index int, op Op,
	args *GetPutAppendArgs, reply *GetPutAppendReply) {
	getOp := "Get"
	emptyValue := ""

	if msg.RequestID == op.RequestID && msg.ClerkID == op.ClerkID {
		reply.Err = OK

		if args.Op == getOp {
			kv.mu.Lock()

			reply.Value, reply.Err = emptyValue, ErrNoKey

			if value, ok := kv.keyValueMap[args.Key]; ok {
				reply.Value, reply.Err = value, OK
			} else {
				reply.Value, reply.Err = emptyValue, ErrNoKey
			}

			delete(kv.requestChanMap, index)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyPutAppend(op *Op) {
	putOp := "Put"
	AppendOp := "Append"

	switch op.Command {
	case putOp:
		kv.keyValueMap[op.Key] = op.Value
	case AppendOp:
		kv.keyValueMap[op.Key] += op.Value
	}
}

func (kv *KVServer) applyRaftLogEntries() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.SnapshotValid {
			kv.applySnapshotIfValid(msg)

		} else if msg.CommandValid {
			kv.applyCommandIfValid(msg)
		}
	}
}

func (kv *KVServer) applySnapshotIfValid(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.SnapshotIndex > kv.lastApplied {
		kv.readPersist(msg.Snapshot)
		kv.lastApplied = msg.SnapshotIndex
	}
}

func (kv *KVServer) applyCommandIfValid(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := msg.Command.(Op)

	if msg.CommandIndex <= kv.lastApplied {
		return
	}

	kv.lastApplied = msg.CommandIndex
	channel := kv.getChannelForIndex(kv.lastApplied)
	clerkRequestID := kv.requestIDMap[op.ClerkID]

	if op.RequestID > clerkRequestID {
		kv.applyPutAppend(&op)
		kv.requestIDMap[op.ClerkID] = op.RequestID
	}

	raftStateSize := kv.rf.Persister.RaftStateSize()
	maxRaftState := kv.maxraftstate

	if maxRaftState > 0 && raftStateSize > maxRaftState {
		kv.rf.Snapshot(msg.CommandIndex, kv.persist())
	}

	channel <- op
}

func (kv *KVServer) getChannelForIndex(index int) chan Op {
	if channel, ok := kv.requestChanMap[index]; ok {
		return channel
	}
	channel := make(chan Op, 1)
	kv.requestChanMap[index] = channel
	return channel
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

	kv.requestIDMap = make(map[int64]int64)
	kv.requestChanMap = make(map[int]chan Op)
	kv.keyValueMap = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := persister.ReadSnapshot()
	kv.readPersist(snapshot)

	go kv.applyRaftLogEntries()

	return kv
}

func (kv *KVServer) persist() []byte {

	snapshotBuffer := new(bytes.Buffer)
	stateEncoder := labgob.NewEncoder(snapshotBuffer)

	requestIDMapErr := stateEncoder.Encode(kv.requestIDMap)
	if requestIDMapErr != nil {
		return nil
	}

	keyValueMapErr := stateEncoder.Encode(kv.keyValueMap)
	if keyValueMapErr != nil {
		return nil
	}

	snapshot := snapshotBuffer.Bytes()
	return snapshot
}

func (kv *KVServer) readPersist(snapshot []byte) {
	if len(snapshot) > 0 {

		var requestIDNewMap map[int64]int64

		snapshotBuffer := bytes.NewBuffer(snapshot)
		stateDecoder := labgob.NewDecoder(snapshotBuffer)

		if stateDecoder.Decode(&requestIDNewMap) != nil ||
			stateDecoder.Decode(&kv.keyValueMap) != nil {

			panic("Failed to decode persistent state from snapshot")

		} else {
			kv.requestIDMap = requestIDNewMap
		}

	}
}
