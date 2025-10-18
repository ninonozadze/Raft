package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu        sync.Mutex
	mp        map[string]string // Key, Value
	requestMp map[int64]int     // Clerk id, RPC id
	returnMp  map[int64]string  // Clerk id, Value
}

// Get Get(key) fetches the current value for the key
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if val, exists := kv.mp[args.Key]; !exists {
		//A Get for a non-existent key should return an empty string
		reply.Value = ""
	} else {
		//Get(key) fetches the current value for the key
		reply.Value = val
	}
}

// Put Put(key, value) installs or replaces the value for a particular key in the map
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.requestMp[args.ClerkID] != args.RequestID {
		kv.mp[args.Key] = args.Value
		kv.requestMp[args.ClerkID] = args.RequestID
	}
}

// Append Append(key, arg) appends arg to key's value and returns the old value
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.requestMp[args.ClerkID] != args.RequestID {
		oldVal := kv.mp[args.Key]
		reply.Value = oldVal

		newVal := args.Value
		kv.mp[args.Key] += newVal

		kv.requestMp[args.ClerkID] = args.RequestID
		kv.returnMp[args.ClerkID] = reply.Value
	} else {
		replyVal := kv.returnMp[args.ClerkID]
		reply.Value = replyVal
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.mp = make(map[string]string)
	kv.requestMp = make(map[int64]int)
	kv.returnMp = make(map[int64]string)
	return kv
}
