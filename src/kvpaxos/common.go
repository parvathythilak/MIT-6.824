package kvpaxos

import "hash/fnv"
import "crypto/rand"
import "math/big"

const (
    OK           = "OK"
    ErrNoKey     = "ErrNoKey"
    NoOp         = "NOOP"
    ErrPut       = "Put Err"
    ErrGet       = "Get Err"
    PUT          = "Put"
    GET          = "Get"
    PUTHASH      = "PutHash"
    ErrOutOfDate = "OutOfDate"
)

type Err string

type PutArgs struct {
    Key    string
    Value  string
    DoHash bool // For PutHash

    From   string // ck.me
    SeqNum int    // sequence number
}

type PutReply struct {
    Err           Err
    PreviousValue string // For PutHash
}

type GetArgs struct {
    Key string

    From   string
    SeqNum int
}

type GetReply struct {
    Err   Err
    Value string
}

func hash(s string) uint32 {
    h := fnv.New32a()
    h.Write([]byte(s))
    return h.Sum32()
}

func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}
