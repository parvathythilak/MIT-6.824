package kvpaxos

import "net/rpc"
import "fmt"
import "strconv"
import "math/rand"

type Clerk struct {
    servers []string

    me  string
    seq int
}

func MakeClerk(servers []string) *Clerk {
    ck := new(Clerk)
    ck.servers = servers

    ck.seq = 0
    ck.me = strconv.FormatInt(nrand(), 10)

    return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
    args interface{}, reply interface{}) bool {
    c, errx := rpc.Dial("unix", srv)
    if errx != nil {
        return false
    }
    defer c.Close()

    err := c.Call(rpcname, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
    var reply GetReply
    args := &GetArgs{key, ck.me, ck.seq}
    to := rand.Int() % len(ck.servers)

    ck.seq += 1 // unique sequence number

    ok := call(ck.servers[to], "KVPaxos.Get", args, &reply)
    for !ok || reply.Err != OK {
        if reply.Err == ErrNoKey {
            return ""
        }
        // try next server
        to = (to + 1) % len(ck.servers)
        ok = call(ck.servers[to], "KVPaxos.Get", args, &reply)
    }
    return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
    var reply PutReply
    args := &PutArgs{key, value, dohash, ck.me, ck.seq}
    to := rand.Int() % len(ck.servers)

    ck.seq += 1 // unique sequence number

    ok := call(ck.servers[to], "KVPaxos.Put", args, &reply)
    for !ok || reply.Err != OK {
        to = (to + 1) % len(ck.servers)
        ok = call(ck.servers[to], "KVPaxos.Put", args, &reply)
    }
    return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
    ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
    v := ck.PutExt(key, value, true)
    return v
}
