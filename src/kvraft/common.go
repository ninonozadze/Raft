package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

type Err string

type GetPutAppendArgs struct {
	Key       string
	Value     string
	RequestID int64
	ClerkID   int64
	Op        string
}

type GetPutAppendReply struct {
	Err   Err
	Value string
}
