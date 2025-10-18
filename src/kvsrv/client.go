package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	server    *labrpc.ClientEnd
	clerkID   int64 // Unique ID for each clerk
	requestID int   // RPC request counter, incremented with each request
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clerkID = nrand() // Assign a unique ClerkID
	ck.requestID = 1     // Initialize RPCID
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	clerkId := ck.clerkID
	rpcId := ck.requestID

	getArgs := GetArgs{Key: key, ClerkID: clerkId, RequestID: rpcId}
	var getReply GetReply

	for {
		ok := ck.server.Call("KVServer.Get", &getArgs, &getReply)
		if ok {
			ck.requestID += 1
			return getReply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	clerkId := ck.clerkID
	rpcId := ck.requestID

	putAppendArgs := PutAppendArgs{Key: key, Value: value, ClerkID: clerkId, RequestID: rpcId}
	var putAppendReply PutAppendReply

	for {
		ok := ck.server.Call("KVServer."+op, &putAppendArgs, &putAppendReply)
		if ok {
			ck.requestID += 1
			return putAppendReply.Value
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
