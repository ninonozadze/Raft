package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	requestID int64
	clerkID   int64
	leaderID  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.requestID = 1
	ck.clerkID = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	requestID := ck.requestID
	clerkID := ck.clerkID
	leaderID := ck.leaderID
	value := ""
	op := "Get"

	getArgs := &GetPutAppendArgs{
		Key:       key,
		Value:     value,
		RequestID: requestID,
		ClerkID:   clerkID,
		Op:        op,
	}

	for {
		getReply := &GetPutAppendReply{}
		ok := ck.servers[leaderID].Call("KVServer.HandleAnyOperation", getArgs, getReply)
		if ok {
			if getReply.Err == ErrNoKey || getReply.Err == OK {
				ck.requestID += 1
				ck.leaderID = leaderID
				if getReply.Err == ErrNoKey {
					return ""
				} else if getReply.Err == OK {
					return getReply.Value
				}
			}
		}
		leaderID = nextLeader(leaderID, len(ck.servers))
		time.Sleep(30 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestID := ck.requestID
	clerkID := ck.clerkID
	leaderID := ck.leaderID

	putAppendArgs := &GetPutAppendArgs{
		Key:       key,
		Value:     value,
		RequestID: requestID,
		ClerkID:   clerkID,
		Op:        op,
	}
	for {
		putAppendReply := &GetPutAppendReply{}
		ok := ck.servers[leaderID].Call("KVServer.HandleAnyOperation", putAppendArgs, putAppendReply)
		if ok {
			if putAppendReply.Err == ErrNoKey || putAppendReply.Err == OK {
				ck.requestID += 1
				ck.leaderID = leaderID
				return
			}
		}
		leaderID = nextLeader(leaderID, len(ck.servers))
		time.Sleep(30 * time.Millisecond)
	}
}

func nextLeader(current int, total int) int {
	return (current + 1) % total
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
