package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

const (
    GET      = "Get"
    PUT      = "Put"
    PUTHASH  = "PutHash"
    RECONFIG = "Reconfigure"
)

type Op struct {
    OpType    string
    OpID      int64
    Key       string
    Value     string
    ConfigNum int
    // for reConfig
    KvData   map[string]string
    PreReply map[int64]string
}

type ShardKV struct {
    mu         sync.Mutex
    l          net.Listener
    me         int
    dead       bool // for testing
    unreliable bool // for testing
    sm         *shardmaster.Clerk
    px         *paxos.Paxos

    gid int64 // my replica group ID

    config          shardmaster.Config
    kvData          map[string]string
    preReply        map[int64]string
    maxProcessedSeq int
}

func (kv *ShardKV) Get(args *GetArgs, reply *Reply) error {
    kv.mu.Lock()
    defer func() {
        DPrintf("%d.%d.%d) Put Returns: %s (%s)\n", kv.gid, kv.me, kv.config.Num, reply.Value, reply.Err)
        kv.mu.Unlock()
    }()
    reply.Err = ErrWrongGroup
    op := Op{}
    op.OpType = GET
    op.OpID = args.ID
    op.Key = args.Key

    kv.proposeOp(op, reply)
    return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *Reply) error {
    kv.mu.Lock()
    defer func() {
        if reply.Err == ErrNoKey {
            reply.Err = OK
        }
        DPrintf("%d.%d.%d) Put Returns: %s (%s)\n", kv.gid, kv.me, kv.config.Num, reply.Value, reply.Err)
        kv.mu.Unlock()
    }()
    reply.Err = ErrWrongGroup
    op := Op{}
    op.OpID = args.ID
    op.Key = args.Key
    op.Value = args.Value
    op.OpType = PUT
    if args.DoHash {
        op.OpType = PUTHASH
    }

    kv.proposeOp(op, reply)

    return nil
}

func (kv *ShardKV) doLog(maxSeq int) {
    if maxSeq <= kv.maxProcessedSeq+1 {
        return
    }

    for i := kv.maxProcessedSeq + 1; i < maxSeq; i++ {
        to := 10 * time.Millisecond
        start := false
        for !kv.dead {
            decided, opp := kv.px.Status(i)
            if decided {
                op := opp.(Op)
                if op.OpType == GET {
                    kv.preReply[op.OpID] = kv.kvData[op.Key]
                } else if op.OpType == PUT {
                    kv.preReply[op.OpID] = kv.kvData[op.Key]
                    kv.kvData[op.Key] = op.Value
                } else if op.OpType == PUTHASH {
                    kv.preReply[op.OpID] = kv.kvData[op.Key]
                    kv.kvData[op.Key] = strconv.Itoa(int(hash(kv.kvData[op.Key] + op.Value)))
                } else if op.OpType == RECONFIG {
                    for k, v := range op.KvData {
                        kv.kvData[k] = v
                    }
                    for k, v := range op.PreReply {
                        kv.preReply[k] = v
                    }
                    kv.config = kv.sm.Query(op.ConfigNum)
                }
                break
            } else if !start {
                kv.px.Start(i, Op{})
                start = true
            }
            time.Sleep(to)
            if to < 10*time.Second {
                to *= 2
            }
        }
    }
    kv.maxProcessedSeq = maxSeq - 1
    kv.px.Done(kv.maxProcessedSeq)
}

func (kv *ShardKV) proposeOp(op Op, reply *Reply) {
    for !kv.dead {
        seq := kv.px.Max() + 1
        kv.doLog(seq)
        if kv.config.Shards[key2shard(op.Key)] != kv.gid {
            return
        }
        if v, found := kv.preReply[op.OpID]; found {
            if v == "" {
                reply.Err = ErrNoKey
            } else {
                reply.Err = OK
            }
            reply.Value = v
            return
        }
        // propose an Op
        kv.px.Start(seq, op)
        to := 10 * time.Millisecond
        for !kv.dead {
            if decided, _ := kv.px.Status(seq); decided {
                seq := kv.px.Max() + 1
                kv.doLog(seq)
                if kv.config.Shards[key2shard(op.Key)] != kv.gid {
                    return
                }

                if v, found := kv.preReply[op.OpID]; found {
                    if v == "" {
                        reply.Err = ErrNoKey
                    } else {
                        reply.Err = OK
                    }
                    reply.Value = v
                    return
                } else {
                    break
                }
            }

            time.Sleep(to)
            if to < 10*time.Second {
                to *= 2
            }
        }
    }
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    seq := kv.px.Max() + 1
    kv.doLog(seq)

    newConfig := kv.sm.Query(kv.config.Num + 1)
    if newConfig.Num == kv.config.Num {
        return
    }
    var gained []int       // the Shard me Gained
    var remoteGained []int // the remote Shard me Gained
    var lost []int         // the Shard me Lost

    for k, v := range newConfig.Shards {
        if kv.config.Shards[k] == kv.gid && v != kv.gid {
            lost = append(lost, k)
        } else if kv.config.Shards[k] != kv.gid && v == kv.gid {
            gained = append(gained, k)
            if kv.config.Shards[k] > 0 {
                remoteGained = append(remoteGained, k)
            }
        }
    }

    newKvData := make(map[string]string)
    newPreReply := make(map[int64]string)

    if len(remoteGained) != 0 && !kv.dead {
        for _, shard := range remoteGained {
            otherGID := kv.config.Shards[shard]
            servers := kv.config.Groups[otherGID]
            args := &FetchArgs{newConfig.Num, shard}
        srvloop:
            for !kv.dead {
                for sid, srv := range servers {
                    var reply FetchReply
                    ok := call(srv, "ShardKV.Fetch", args, &reply)
                    if ok && (reply.Err == OK) {
                        for k, v := range reply.KvData {
                            newKvData[k] = v
                        }
                        for k, v := range reply.PreReply {
                            newPreReply[k] = v
                        }
                        break srvloop
                    } else {
                        DPrintf("%d.%d.%d) Failed to get Shard %d from %d.%d\n", kv.gid, kv.me, kv.config.Num, shard, otherGID, sid)
                    }
                }
                time.Sleep(10 * time.Millisecond)
            }
        }
    }

    kv.proposeConfig(newConfig.Num, newKvData, newPreReply)

}

//
// fetch the data of given shardid
//
func (kv *ShardKV) Fetch(args *FetchArgs, reply *FetchReply) error {
    if kv.config.Num < args.Config {
        reply.Err = ErrNoKey
        return nil
    }

    shardStore := make(map[string]string)

    for k, v := range kv.kvData {
        if key2shard(k) == args.Shard {
            shardStore[k] = v
        }
    }

    reply.Err = OK
    reply.KvData = shardStore
    reply.PreReply = kv.preReply
    return nil
}

func (kv *ShardKV) proposeConfig(num int, kvData map[string]string, preReply map[int64]string) {
    op := Op{}
    op.OpType = RECONFIG
    op.OpID = int64(num)
    op.ConfigNum = num
    op.KvData = kvData
    op.PreReply = preReply

    for !kv.dead {
        seq := kv.px.Max() + 1
        kv.doLog(seq)
        if kv.config.Num >= num {
            return
        }

        kv.px.Start(seq, op)
        to := 10 * time.Millisecond
        for !kv.dead {
            if decided, _ := kv.px.Status(seq); decided {
                seq := kv.px.Max() + 1
                kv.doLog(seq)
                if kv.config.Num >= num {
                    return
                } else {
                    break
                }
            }

            time.Sleep(to)
            if to < 10*time.Second {
                to *= 2
            }
        }
    }
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
    kv.dead = true
    kv.l.Close()
    kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
    servers []string, me int) *ShardKV {
    gob.Register(Op{})

    kv := new(ShardKV)
    kv.me = me
    kv.gid = gid
    kv.sm = shardmaster.MakeClerk(shardmasters)

    kv.config = kv.sm.Query(0)
    kv.kvData = make(map[string]string)
    kv.preReply = make(map[int64]string)
    kv.maxProcessedSeq = -1

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
                fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
                kv.kill()
            }
        }
    }()

    go func() {
        for kv.dead == false {
            kv.tick()
            time.Sleep(250 * time.Millisecond)
        }
    }()

    return kv
}
