package pbservice

import "hash/fnv"
import "crypto/rand"
import "math/big"

const (
    OK             = "OK"
    ErrNoKey       = "ErrNoKey"
    ErrWrongServer = "ErrWrongServer"
    ErrSplitBrain  = "SplitBrain"
    ErrForwarded   = "Error: Command not from primary"
)

type Err string

type PutArgs struct {
    Key    string
    Value  string
    DoHash bool // For PutHash

    // Field names must start with capital letters,
    // otherwise RPC will break.
    ClientID  int64
    SeqNum    int64
    Forwarded bool // whether request is forwarded from primary, to Indentity 'split brain'
}

type PutReply struct {
    Err           Err
    PreviousValue string // For PutHash
}

type GetArgs struct {
    Key string

    ClientID  int64
    SeqNum    int64
    Forwarded bool
}

type GetReply struct {
    Err   Err
    Value string
}

type UpdateDatabaseArgs struct {
    Caller   string                     // To check that the primary is calling
    KvData   map[string]string          // The entire key value database
    PreReply map[int64]map[int64]string // Past replays: clientId -> (seqNum->replyValue)
}

type UpdateDatabaseReply struct {
    Err Err
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
