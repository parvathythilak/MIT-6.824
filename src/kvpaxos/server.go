package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

type Op struct {
    From          string // clerk.me
    ClerkSeq      int    // clerk's request sequence number
    PaxosSeq      int    // paxos sequence number
    Type          string // put/puthash/get
    Key           string
    Value         string
    PreviousValue string
    Err           Err
}

type KVPaxos struct {
    mu         sync.Mutex
    l          net.Listener
    me         int
    dead       bool // for testing
    unreliable bool // for testing
    px         *paxos.Paxos

    preReply      map[string]*Op    // past responses: From->Op
    kvData        map[string]string // Key-Value Map
    seqChan       map[int]chan *Op  // seq->channel
    maxInstanceID int
}

//
// get one unique and outstanding sequence number and the channel it corresponding to.
//
func (kv *KVPaxos) getSeqChan() (int, chan *Op) {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    seq := max(kv.px.Max()+1, kv.maxInstanceID+1)
    for _, ok := kv.seqChan[seq]; ok; _, ok = kv.seqChan[seq] {
        seq++
    }
    ch := make(chan *Op)
    kv.seqChan[seq] = ch
    return seq, ch
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
    seq, ch := kv.getSeqChan()

    proposedOp := Op{From: args.From, ClerkSeq: args.SeqNum, PaxosSeq: seq,
        Type: Get, Key: args.Key}
    kv.px.Start(seq, proposedOp)

    resultOp := <-ch

    if resultOp.From != args.From || resultOp.ClerkSeq != args.SeqNum {
        // end this sequence
        reply.Err = ErrGet
        kv.mu.Lock()
        close(ch)
        delete(kv.seqChan, seq)
        kv.mu.Unlock()
        return nil
    }
    reply.Err = resultOp.Err
    reply.Value = resultOp.Value
    kv.mu.Lock()
    close(ch)
    delete(kv.seqChan, seq)
    kv.mu.Unlock()
    return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
    seq, ch := kv.getSeqChan()
    tp := PUT
    if args.DoHash {
        tp := PUTHASH
    }

    proposedOp := Op{From: args.From, ClerkSeq: args.SeqNum, PaxosSeq: seq,
        Type: tp, Key: args.Key, Value: args.Value}
    kv.px.Start(seq, proposedOp)
    resultOp := <-ch
    if resultOp.From != args.From || resultOp.ClerkSeq != args.SeqNum {
        reply.Err = ErrPut
        kv.mu.Lock()
        close(ch)
        delete(kv.seqChan, seq)
        kv.mu.Unlock()
        return nil
    }

    reply.Err = resultOp.Err
    reply.PreviousValue = resultOp.PreviousValue
    kv.mu.Lock()
    close(ch)
    delete(kv.seqChan, seq)
    kv.mu.Unlock()
    return nil
}

//
// do the Get/Put/PutHash oprations
//
func (kv *KVPaxos) doOp(seq int, op *Op) {
    isHandled := fasle
    preOp := &Op{}
    if dup, ok := kv.preReply[op.From]; ok {
        if dup.ClerkSeq == op.ClerkSeq {
            // the sequence is handled, and it's the newest
            isHandled = true
            preOp = dup
        } else if dup.ClerkSeq > op.ClerkSeq {
            // the sequence is out-of-date
            isHandled = true
            preOp = &Op{From: NoOp}
        }
    }
    if !isHandled {
        // handle it!
        if op.Type == GET {
            if val, ok := kv.kvData[op.Key]; ok {
                op.Value = v
                op.Err = OK
            } else {
                op.Err = ErrNoKey
            }
        } else if op.Type == PUT {
            op.PreviousValue = kv.kvData[op.Key]
            kv.kvData[op.Key] = op.Value
            op.Err = OK
        } else if op.Type == PUTHASH {
            prevVal := kv.kvData[op.Key]
            op.PreviousValue = prevVal
            kv.kvData[op.Key] = strconv.Itoa(int(hash(prevVal + op.Value)))
            op.Err = OK
        }
        // save the current result
        kv.preReply[op.From] = op
    } else {
        if preOp.From == NoOp {
            // out-of-date opration
            op.Err = ErrOutOfDate
        } else {
            op = preOp
        }
    }
    kv.maxInstanceID = seq
    kv.px.Done(seq)
}

//
// wait for Paxos instances to complete agreement
//
func (kv *KVPaxos) updateStatus() {
    seq := 0
    sleepTime := 10 * time.Millisecond
    isEnd := false
    for !kv.dead {
        decided, val := kv.px.Status(seq)
        if decided {
            op := val.(Op)
            kv.mu.Lock()
            if op.From != NoOp {
                // do Get/Put/PutHash
                doOp(seq, &op)
            } else {
                // the instance is ended
                kv.maxInstanceID = seq
                kv.px.Done(seq)
            }
            if ch, ok := kv.seqChan[seq]; ok {
                ch <- &op
            }
            kv.mu.Unlock()
            seq++
            isEnd = false
        } else {
            time.Sleep(sleepTime)
            if sleepTime < 25*time.Millisecond {
                sleepTime *= 2
            } else {
                if !isEnd {
                    // we've slept Long enough, end this sequence loop
                    sleepTime = 10 * time.Millisecond
                    kv.px.Start(Seq, Op{From: NoOp})
                    time.Sleep(time.Millisecond)
                    isEnd = true
                }
            }
        }
    }
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
    DPrintf("Kill(%d): die\n", kv.me)
    kv.dead = true
    kv.l.Close()
    kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
    // call gob.Register on structures you want
    // Go's RPC library to marshall/unmarshall.
    gob.Register(Op{})

    kv := new(KVPaxos)
    kv.me = me

    // initialization
    kv.kvData = make(map[string]string)
    kv.preReply = make(map[string]*Op)
    kv.seqChan = make(map[int]chan *Op)
    kv.maxInstanceID = -1

    go kv.updateStatus()

    rpcs := rpc.NewServer()
    rpcs.Register(kv)

    kv.px = paxos.Make(servers, me, rpcs)

    os.Remove(servers[me])
    l, e := net.Listen("unix", servers[me])
    if e != nil {
        log.Fatal("listen error: ", e)
    }
    kv.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    go func() {
        for kv.dead == false {
            conn, err := kv.l.Accept()
            if err == nil && kv.dead == false {
                if kv.unreliable && (rand.Int63()%1000) < 100 {
                    // discard the request.
                    conn.Close()
                } else if kv.unreliable && (rand.Int63()%1000) < 200 {
                    // process the request but force discard of reply.
                    c1 := conn.(*net.UnixConn)
                    f, _ := c1.File()
                    err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
                    if err != nil {
                        fmt.Printf("shutdown: %v\n", err)
                    }
                    go rpcs.ServeConn(conn)
                } else {
                    go rpcs.ServeConn(conn)
                }
            } else if err == nil {
                conn.Close()
            }
            if err != nil && kv.dead == false {
                fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
                kv.kill()
            }
        }
    }()

    return kv
}

func max(a, b int) int {
    if a < b {
        return b
    }
    return a
}
