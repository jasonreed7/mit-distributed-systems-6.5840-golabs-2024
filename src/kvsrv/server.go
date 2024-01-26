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
	mu sync.Mutex

	// Your definitions here.
	kVMap        map[string]string
	prevRequests map[int64]requestHistoryEntry
}

type requestHistoryEntry struct {
	requestCounter int
	value          string
}

func (kv *KVServer) checkDuplicate(clientId int64, requestCounter int) (string, bool) {
	// Your code here.
	prevEntry, ok := kv.prevRequests[clientId]

	if !ok || prevEntry.requestCounter != requestCounter {
		return "", false
	}

	return prevEntry.value, true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.kVMap[args.Key]

	if !ok {
		val = ""
	}

	reply.Value = val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId := args.ClientId
	requestCounter := args.RequestCounter
	prevVal, isDuplicate := kv.checkDuplicate(clientId, requestCounter)

	if isDuplicate {
		reply.Value = prevVal
		return
	}

	value := args.Value

	kv.kVMap[args.Key] = value

	kv.prevRequests[clientId] = requestHistoryEntry{requestCounter, ""}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId := args.ClientId
	requestCounter := args.RequestCounter
	prevVal, isDuplicate := kv.checkDuplicate(clientId, requestCounter)

	if isDuplicate {
		reply.Value = prevVal
		return
	}

	key := args.Key
	appendVal := args.Value

	currVal, ok := kv.kVMap[key]

	var newVal string
	if ok {
		newVal = currVal + appendVal
		reply.Value = currVal
	} else {
		newVal = appendVal
		currVal = ""
		reply.Value = currVal
	}

	kv.kVMap[key] = newVal

	kv.prevRequests[clientId] = requestHistoryEntry{requestCounter, currVal}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.kVMap = make(map[string]string)
	kv.prevRequests = make(map[int64]requestHistoryEntry)

	return kv
}
