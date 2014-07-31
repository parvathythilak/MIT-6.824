package shardmaster

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

type ShardMaster struct {
    mu         sync.Mutex
    l          net.Listener
    me         int
    dead       bool // for testing
    unreliable bool // for testing
    px         *paxos.Paxos

    configs         []Config // indexed by config num
    maxConfigNum    int
    maxProcessedSeq int
}

const (
    JOIN  = "Join"
    LEAVE = "Leave"
    MOVE  = "Move"
    QUERY = "Query"
)

type Op struct {
    Op      string
    GID     int64
    Servers []string
    Shard   int
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

//
// The Join RPC's arguments are a unique non-zero replica group identifier (GID) and
// an array of server ports. The shardmaster should react by creating a new configuration
// that includes the new replica group. The new configuration should divide the shards
// as evenly as possible among the groups, and should move as few shards as possible to
// achieve that goal.
//
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
    sm.mu.Lock()

    op := Op{JOIN, args.GID, args.Servers, 0}

    for !sm.dead {
        seq := sm.px.Max() + 1
        sm.px.Start(seq, op)
        //to := 10 * time.Millisecond
        for !sm.dead {
            decided, replyOp := sm.px.Status(seq)
            if decided {
                resultOp := replyOp.(Op)
                sm.doLog(seq + 1)
                if resultOp.Op == op.Op && resultOp.GID == op.GID && resultOp.Shard == op.Shard {
                    sm.mu.Unlock()
                    return nil
                } else {
                    break
                }
            }
            time.Sleep(20 * time.Millisecond)
        }
    }
    sm.mu.Unlock()
    return nil
}

//
// The Leave RPC's arguments are the GID of a previously joined group.
// The shardmaster should create a new configuration that does not include the group,
// and that assigns the group's shards to the remaining groups.
// The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal.
//
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
    sm.mu.Lock()

    op := Op{LEAVE, args.GID, nil, 0}

    for !sm.dead {
        seq := sm.px.Max() + 1
        sm.px.Start(seq, op)

        for !sm.dead {
            decided, replyOp := sm.px.Status(seq)
            if decided {
                resultOp := replyOp.(Op)
                sm.doLog(seq + 1)
                if resultOp.Op == op.Op && resultOp.GID == op.GID && resultOp.Shard == op.Shard {
                    sm.mu.Unlock()
                    return nil
                } else {
                    break
                }
            }
            time.Sleep(20 * time.Millisecond)
        }
    }
    sm.mu.Unlock()
    return nil
}

//
// The Move RPC's arguments are a shard number and a GID. The shardmaster should create
// a new configuration in which the shard is assigned to the group. The main purpose of Move
// is to allow us to test your software, but it might also be useful to fine-tune load balance
// if some shards are more popular than others or some replica groups are slower than others.
// A Join or Leave following a Move will likely un-do the Move, since Join and Leave re-balance.
//
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
    sm.mu.Lock()

    op := Op{MOVE, args.GID, nil, args.Shard}

    for !sm.dead {
        seq := sm.px.Max() + 1
        sm.px.Start(seq, op)

        for !sm.dead {
            decided, replyOp := sm.px.Status(seq)
            if decided {
                resultOp := replyOp.(Op)
                sm.doLog(seq + 1)
                if resultOp.Op == op.Op && resultOp.GID == op.GID && resultOp.Shard == op.Shard {
                    sm.mu.Unlock()
                    return nil
                } else {
                    break
                }
            }
            time.Sleep(20 * time.Millisecond)
        }
    }
    sm.mu.Unlock()
    return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
    sm.mu.Lock()

    op := Op{QUERY, int64(args.Num), nil, 0}

    for !sm.dead {
        seq := sm.px.Max() + 1
        if args.Num > sm.maxConfigNum {
            op = Op{QUERY, -1, nil, 0}
        }
        sm.px.Start(seq, op)

        for !sm.dead {
            decided, _ := sm.px.Status(seq)
            if decided {
                sm.doLog(seq + 1)
                if args.Num >= 0 && args.Num < sm.maxConfigNum {
                    reply.Config = sm.configs[args.Num]
                } else {
                    reply.Config = sm.configs[sm.maxConfigNum]
                }
                sm.mu.Unlock()
                return nil
            }
            time.Sleep(20 * time.Millisecond)
        }
    }

    sm.mu.Unlock()
    return nil
}

func (sm *ShardMaster) doLog(maxSeq int) {
    if maxSeq <= sm.maxProcessedSeq+1 {
        return
    }

    for i := sm.maxProcessedSeq + 1; i < maxSeq; i++ {
        to := 10 * time.Millisecond
        start := false
        for !sm.dead {
            decided, opp := sm.px.Status(i)
            if decided {
                op := opp.(Op)
                if op.Op == QUERY {
                    // QUERY
                } else if op.Op == JOIN {
                    sm.createConfig(JOIN, op.GID, op.Servers, -1)
                } else if op.Op == LEAVE {
                    sm.createConfig(LEAVE, op.GID, nil, -1)
                } else if op.Op == MOVE {
                    sm.createConfig(MOVE, op.GID, nil, op.Shard)
                }
                break
            } else if !start {
                sm.px.Start(i, Op{QUERY, -1, nil, 0})
                start = true
            }
            time.Sleep(to)
            if to < 10*time.Second {
                to *= 2
            }
        }
    }
    sm.maxProcessedSeq = maxSeq - 1
    sm.px.Done(sm.maxProcessedSeq)
}

func (sm *ShardMaster) createConfig(method string, gid int64, servers []string, shard int) {
    oldConfig := sm.configs[sm.maxConfigNum]
    sm.maxConfigNum = sm.maxConfigNum + 1
    newConfig := Config{}
    newConfig.Num = sm.maxConfigNum
    newConfig.Groups = map[int64][]string{}
    var gids []int64

    switch method {
    case JOIN:
        for k, v := range oldConfig.Groups {
            if k != gid && k != 0 {
                gids = append(gids, k)
                newConfig.Groups[k] = v
            }
        }
        gids = append(gids, gid)
        newConfig.Groups[gid] = servers

        newConfig.Shards = sm.balance(gids, oldConfig.Shards)

        sm.configs = append(sm.configs, newConfig)
        DPrintf("JOIN...")
    case LEAVE:
        for k, v := range oldConfig.Groups {
            if k != gid && k != 0 {
                gids = append(gids, k)
                newConfig.Groups[k] = v
            }
        }

        newConfig.Shards = sm.balance(gids, oldConfig.Shards)

        sm.configs = append(sm.configs, newConfig)
        DPrintf("LEAVE...")
    case MOVE:
        for k, v := range oldConfig.Shards {
            if k == shard {
                newConfig.Shards[k] = gid
            } else {
                newConfig.Shards[k] = v
            }
        }

        for k, v := range oldConfig.Groups {
            newConfig.Groups[k] = v
        }

        sm.configs = append(sm.configs, newConfig)
        DPrintf("MOVE...")
    }
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
    sm.dead = true
    sm.l.Close()
    sm.px.Kill()
}

func (sm *ShardMaster) balance(gids []int64, shards [NShards]int64) [NShards]int64 {

    expectedGPS := (NShards / len(gids))
    if expectedGPS <= 0 {
        expectedGPS = 1
    }
    var newShards [NShards]int64
    num := make(map[int64]int)
    var over []int

    for k, v := range shards { // shardid -> gid
        num[v] = num[v] + 1
        newShards[k] = v
        found := false
        for _, v2 := range gids {
            if v == v2 {
                found = true
            }
        }
        if num[v] > expectedGPS || !found {
            over = append(over, k)
        }
    }
    for _, k := range gids {
        if k == 0 {
            continue
        }
        for num[k] < expectedGPS && len(over) > 0 {
            newShards[over[0]] = k
            num[k] += 1
            over = over[1:len(over)]
        }
    }

    return newShards
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
    gob.Register(Op{})

    sm := new(ShardMaster)
    sm.me = me

    sm.configs = make([]Config, 1)
    sm.configs[0].Groups = map[int64][]string{}

    sm.maxProcessedSeq = -1
    sm.maxConfigNum = 0

    rpcs := rpc.NewServer()
    rpcs.Register(sm)

    sm.px = paxos.Make(servers, me, rpcs)

    os.Remove(servers[me])
    l, e := net.Listen("unix", servers[me])
    if e != nil {
        log.Fatal("listen error: ", e)
    }
    sm.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    go func() {
        for sm.dead == false {
            conn, err := sm.l.Accept()
            if err == nil && sm.dead == false {
                if sm.unreliable && (rand.Int63()%1000) < 100 {
                    // discard the request.
                    conn.Close()
                } else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
            if err != nil && sm.dead == false {
                fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
                sm.Kill()
            }
        }
    }()

    return sm
}
