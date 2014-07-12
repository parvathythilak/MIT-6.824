package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

//
// one Paxos instance
//
type Proposal struct {
    prepareID int         // prepare_n
    acceptID  int         // accept_n
    value     interface{} // accept_v
    decided   bool        // whether the proposal is decided or not
}

const (
    PrepareOK     = "PrepareOK"
    PrepareReject = "PrepareReject"
    AcceptOK      = "AcceptOK"
    AcceptReject  = "AcceptReject"
    DecideOK      = "DecideOK"
)

type ReplyType string

type Paxos struct {
    mu         sync.Mutex
    l          net.Listener
    dead       bool
    unreliable bool
    rpcCount   int
    peers      []string
    me         int // index into peers[]

    instances     map[int]Proposal // seq->instance
    done          map[int]int      // index->max done n
    maxInstanceID int
}

type PrepareArgs struct {
    InstanceID int
    PrepareID  int
}

type PrepareReply struct {
    PrepareID int         // n_a
    Value     interface{} // v_a
    Done      int         // max done seqID
    Reply     ReplyType
}

type AcceptArgs struct {
    InstanceID int         // seq
    AcceptID   int         // n
    Value      interface{} // v'
}

type AcceptReply struct {
    AcceptID int
    Done     int
    Reply    ReplyType
}

type DecideArgs struct {
    InstanceID int
    DecideID   int
    Value      interface{}
}

type DecideReply struct {
    Done  int
    Reply ReplyType
}

const SleepInterval = time.Millisecond * 100

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
    c, err := rpc.Dial("unix", srv)
    if err != nil {
        err1 := err.(*net.OpError)
        if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
            fmt.Printf("paxos Dial() failed: %v\n", err1)
        }
        return false
    }
    defer c.Close()

    err = c.Call(name, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}

func max(a, b int) int {
    if a < b {
        return b
    }
    return a
}

func (px *Paxos) getProposal(seq int) Proposal {
    px.mu.Lock()
    defer px.mu.Unlock()

    _, ok := px.instances[seq]
    if !ok {
        // new instance
        px.instances[seq] = Proposal{-1, -1, nil, false}
    } else {
        // update maxInstance
        px.maxInstanceID = max(px.maxInstanceID, seq)
    }
    return px.instances[seq]
}

func (px *Paxos) callAcceptor(index int, name string, args interface{}, reply interface{}) bool {
    if index == px.me {
        if name == "Paxos.Prepare" {
            return px.Prepare(args.(*PrepareArgs), reply.(*PrepareReply)) == nil
        } else if name == "Paxos.Accept" {
            return px.Accept(args.(*AcceptArgs), reply.(*AcceptReply)) == nil
        } else if name == "Paxos.Decide" {
            return px.Decide(args.(*DecideArgs), reply.(*DecideReply)) == nil
        }
    }
    return call(px.peers[index], name, args, reply)
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
    go func(seq int, v interface{}) {
        inst := px.getProposal(seq)
        nID := 0
        for !inst.decided && !px.dead {
            highestID := -1
            highestValue := v
            num := 0
            // 1. prepare stage
            for i := 0; i < len(px.peers); i++ {
                args := &PrepareArgs{seq, nID}
                var reply PrepareReply
                if px.callAcceptor(i, "Paxos.Prepare", args, &reply) && reply.Reply == PrepareOK {
                    px.done[i] = reply.Done
                    num += 1
                    if reply.PrepareID > highestID {
                        highestID = reply.PrepareID
                        highestValue = reply.Value
                    }
                }
            }

            if num <= len(px.peers)/2 {
                inst = px.getProposal(seq)
                if highestID > nID {
                    nID = highestID + 1
                } else {
                    nID += 1
                }
                continue
            }
            // 2. accept stage
            num = 0
            for i := 0; i < len(px.peers); i++ {
                args := &AcceptArgs{seq, nID, highestValue}
                var reply AcceptReply
                if px.callAcceptor(i, "Paxos.Accept", args, &reply) && reply.Reply == AcceptOK {
                    px.done[i] = reply.Done
                    num += 1
                }
            }

            if num <= len(px.peers)/2 {
                inst = px.getProposal(seq)
                if highestID > nID {
                    nID = highestID + 1
                } else {
                    nID += 1
                }
                continue
            }

            // 3. decide stage
            for i := 0; i < len(px.peers); i++ {
                args := &DecideArgs{seq, nID, highestValue}
                var reply DecideReply
                if px.callAcceptor(i, "Paxos.Decide", args, &reply) && reply.Reply == DecideOK {
                    px.done[i] = reply.Done
                }
            }
            break
        }
    }(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
    px.done[px.me] = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
    return px.maxInstanceID
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
    min := 1<<32 - 1 // max_int
    for _, v := range px.done {
        if v < min {
            min = v
        }
    }
    return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
    inst := px.getProposal(seq)
    return inst.decided, inst.value
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
    px.dead = true
    if px.l != nil {
        px.l.Close()
    }
}

//
// prepare stage
//
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()

    if _, ok := px.instances[args.InstanceID]; !ok {
        px.instances[args.InstanceID] = Proposal{-1, -1, nil, false}
        px.maxInstanceID = max(px.maxInstanceID, args.InstanceID)
    }

    inst := px.instances[args.InstanceID]
    reply.Done = px.done[px.me]

    if args.PrepareID > inst.prepareID {
        // if n > n_p: n_p = n && reply prepare_ok(n, n_a, v_a)
        px.instances[args.InstanceID] = Proposal{args.PrepareID, inst.acceptID, inst.value, inst.decided}
        reply.Reply = PrepareOK
        reply.PrepareID = inst.acceptID // n_a
        reply.Value = inst.value        // n_v
    } else {
        reply.Reply = PrepareReject
    }
    return nil
}

//
// Accept stage
//
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()
    inst := px.instances[args.InstanceID]
    reply.Done = px.done[px.me]

    if args.AcceptID >= inst.prepareID {
        // if n >= n_p: n_p = n && n_a = n && v_a = v && reply accept_ok(n)
        px.instances[args.InstanceID] = Proposal{args.AcceptID, args.AcceptID, args.Value, inst.decided}
        reply.Reply = AcceptOK
        reply.AcceptID = args.InstanceID
    } else {
        reply.Reply = AcceptReject
    }
    return nil
}

//
// decide stage
//
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
    px.mu.Lock()
    defer px.mu.Unlock()

    px.instances[args.InstanceID] = Proposal{args.DecideID, args.DecideID, args.Value, true}

    reply.Reply = DecideOK
    reply.Done = px.done[px.me]
    return nil
}

//
// release memory for sequence numbers < minSeq
//
func (px *Paxos) releaseMemory(minSeq int) {
    px.mu.Lock()
    defer px.mu.Unlock()

    for seq, _ := range px.instances {
        if seq < minSeq {
            delete(px.instances, seq)
        }
    }

}

//
// a long-running release collector to forget all information
// about any instances it knows that are < Min().
//
func (px *Paxos) MemoryReleasor() {
    preMin := -1
    for !px.dead {
        min := px.Min()
        if preMin < min {
            // release instances from preMin to min
            px.releaseMemory(min)
            preMin = min
        }
        time.Sleep(SleepInterval)
    }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
    px := &Paxos{}
    px.peers = peers
    px.me = me

    px.maxInstanceID = -1
    px.instances = make(map[int]Proposal)
    px.done = make(map[int]int)
    for i := 0; i < len(px.peers); i++ {
        px.done[i] = -1
    }

    go px.MemoryReleasor()

    if rpcs != nil {
        // caller will create socket &c
        rpcs.Register(px)
    } else {
        rpcs = rpc.NewServer()
        rpcs.Register(px)

        // prepare to receive connections from clients.
        // change "unix" to "tcp" to use over a network.
        os.Remove(peers[me]) // only needed for "unix"
        l, e := net.Listen("unix", peers[me])
        if e != nil {
            log.Fatal("listen error: ", e)
        }
        px.l = l

        // please do not change any of the following code,
        // or do anything to subvert it.

        // create a thread to accept RPC connections
        go func() {
            for px.dead == false {
                conn, err := px.l.Accept()
                if err == nil && px.dead == false {
                    if px.unreliable && (rand.Int63()%1000) < 100 {
                        // discard the request.
                        conn.Close()
                    } else if px.unreliable && (rand.Int63()%1000) < 200 {
                        // process the request but force discard of reply.
                        c1 := conn.(*net.UnixConn)
                        f, _ := c1.File()
                        err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
                        if err != nil {
                            fmt.Printf("shutdown: %v\n", err)
                        }
                        px.rpcCount++
                        go rpcs.ServeConn(conn)
                    } else {
                        px.rpcCount++
                        go rpcs.ServeConn(conn)
                    }
                } else if err == nil {
                    conn.Close()
                }
                if err != nil && px.dead == false {
                    fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
                }
            }
        }()
    }

    return px
}
