package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	ClerkID   int64 // Unique ID for each clerk
	RequestID int   // RPC request counter
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key       string
	ClerkID   int64 // Unique ID for each clerk
	RequestID int   // RPC request counter
}

type GetReply struct {
	Value string
}
