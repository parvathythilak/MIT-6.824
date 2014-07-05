package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        n, err = fmt.Printf(format, a...)
    }
    return
}

type PBServer struct {
    l          net.Listener
    dead       bool // for testing
    unreliable bool // for testing
    me         string
    vs         *viewservice.Clerk
    done       sync.WaitGroup
    finish     chan interface{}

    currentView  viewservice.View  // current View
    kvData       map[string]string // Key-Value database
    kvLock       sync.Mutex
    preReply     map[int64]map[int64]string // Past replays: clientId -> (seqNum->replyValue)
    updateBackup bool                       // whether we should update backup server
}

// check this request handled or not
func (pb *PBServer) HandleSequence(clientID int64, seqNum int64) bool {
    client, exists := pb.preReply[clientID]
    if !exists {
        pb.preReply[clientID] = make(map[int64]string)
    }
    _, handled := client[seqNum]
    return handled
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
    pb.kvLock.Lock()
    defer pb.kvLock.Unlock()

    handled := pb.HandleSequence(args.ClientID, args.SeqNum)

    if pb.currentView.Primary != pb.me {
        DPrintf("I(%s) am not primary", pb.me)
        reply.Err = ErrWrongServer
        return nil
    }
    if (pb.currentView.Primary == pb.me) && args.Forwarded {
        // the request is forwarded, cause I'm not backup return error (split brain)
        reply.Err = ErrSplitBrain
        reply.PreviousValue = ""
        return nil
    }
    if (pb.currentView.Primary == pb.me) && handled {
        DPrintf("client %d, sequence %d has been handled.", args.ClientID, args.SeqNum)
        reply.Err = OK
        reply.PreviousValue = pb.preReply[args.ClientID][args.SeqNum]
        return nil
    }
    if (pb.currentView.Primary == pb.me) && !handled {
        // handle request
        if pb.currentView.Backup != "" {
            // forward to backup server until response is OK
            backupReply := new(PutReply)
            args.Forwarded = true
            backupRespond := call(pb.currentView.Backup, "PBServer.PutUpdate", args, &backupReply)
            args.Forwarded = false
            if !backupRespond || (backupReply.Err != OK) || (backupReply.PreviousValue != pb.kvData[args.Key]) {
                // can't RCP to backup, or backup is out-of-date
                // we should update the backup database first.
                pb.updateBackup = true
                reply.PreviousValue = ""
                reply.Err = "ErrForwarded"
                return nil
            }
        }

        reply.PreviousValue = pb.kvData[args.Key]
        pb.kvData[args.Key] = args.Value
        pb.preReply[args.ClientID][args.SeqNum] = args.Value
        reply.Err = OK
        DPrintf("Put() done, no error (Put %s, %s)", args.Key, args.Value)
    }

    return nil
}

// the backup server update its key-value database according to the request of primary
func (pb *PBServer) PutUpdate(args *PutArgs, reply *PutReply) error {
    if pb.me == pb.currentView.Backup {
        if args.Forwarded {
            reply.PreviousValue = pb.kvData[args.Key]
            pb.kvData[args.Key] = args.Value
            pb.HandleSequence(args.ClientID, args.SeqNum)
            pb.preReply[args.ClientID][args.SeqNum] = args.Value
            reply.Err = OK
            DPrintf("... Backup done, no error (Put %s, %s)", args.Key, args.Value)
        } else {
            DPrintf("... Ignoring command, primary")
            reply.Err = "Error: Command not from primary"
            reply.PreviousValue = ""
        }
    } else {
        reply.Err = ErrWrongServer
    }
    return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    pb.kvLock.Lock()
    defer pb.kvLock.Unlock()

    amPrimary := (pb.currentView.Primary == pb.me)

    if !amPrimary {
        reply.Err = ErrWrongServer
        reply.Value = ""
        return nil
    }

    if amPrimary && args.Forwarded {
        reply.Err = ErrSplitBrain
        reply.Value = ""
        return nil
    }

    if amPrimary {
        if pb.currentView.Backup != "" {
            backupReply := new(GetReply)
            args.Forwarded = true
            backupRespond := call(pb.currentView.Backup, "PBServer.GetUpdate", args, &backupReply)
            args.Forwarded = false
            if !backupRespond || (backupReply.Err != OK) || (backupReply.Value != pb.kvData[args.Key]) {
                // we should update backup
                pb.updateBackup = true
                reply.Value = ""
                reply.Err = ErrForwarded
                return nil
            }
        }
        // reply the current value
        reply.Value = pb.kvData[args.Key]
        reply.Err = OK
    }
    return nil
}

func (pb *PBServer) GetUpdate(args *GetArgs, reply *GetReply) error {
    if pb.me == pb.currentView.Backup {
        if args.Forwarded {
            reply.Value = pb.kvData[args.Key]
            reply.Err = OK
            DPrintf("... Backup done, no error, reply is %s", reply.Value)
        } else {
            reply.Err = ErrForwarded
            reply.Value = ""
        }
    } else {
        reply.Err = ErrWrongServer
        reply.Value = ""
    }
    return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
    pb.kvLock.Lock()
    defer pb.kvLock.Unlock()

    // ping viewservice to get the newest p&b
    view, err := pb.vs.Ping(pb.currentView.Viewnum)
    if err != nil {
        log.Println("receive error from Ping: ", err)
        return
    }

    amPrimary := (pb.currentView.Primary == pb.me)
    // amBackup := (pb.currentView.Backup == pb.me)

    if amPrimary && (view.Viewnum != pb.currentView.Viewnum) && (view.Backup != "") {
        // we should update the backup cause me just bacame primary or backup changed
        pb.updateBackup = true
    }

    pb.currentView = view

    if pb.updateBackup {
        pb.UpdateBackupDatabase(pb.currentView.Backup)
    }
}

func (pb *PBServer) UpdateBackupDatabase(backup string) {
    DPrintf("\nUpdating backup server %s", backup)
    updateArgs := new(UpdateDatabaseArgs)
    updateArgs.Caller = pb.me
    updateArgs.KvData = pb.kvData
    updateArgs.PreReply = pb.preReply

    updateReply := new(UpdateDatabaseReply)
    call(backup, "PBServer.UpdateDatabase", updateArgs, &updateReply)
    if updateReply.Err != OK {
        DPrintf("\n\t%s", updateReply.Err)
    } else {
        DPrintf("\n\tDone updating backup %s", backup)
        pb.updateBackup = false
    }
}

// the bakup is new or out of date, should updated
func (pb *PBServer) UpdateDatabase(args *UpdateDatabaseArgs, reply *UpdateDatabaseReply) error {
    pb.kvLock.Lock()
    defer pb.kvLock.Unlock()

    view, _ := pb.vs.Ping(pb.currentView.Viewnum)

    // If I'm not the backup, return error
    if view.Backup != pb.me {
        DPrintf("UpdateDatabase: %s I'm not the backup", pb.me)
        reply.Err = ErrWrongServer
        return nil
    }

    if view.Primary != args.Caller {
        DPrintf("UpdateDatabase: %s caller is not primary", args.Caller)
        reply.Err = ErrWrongServer
        return nil
    }

    pb.kvData = args.KvData
    pb.preReply = args.PreReply

    return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
    pb.dead = true
    pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
    pb := new(PBServer)
    pb.me = me
    pb.vs = viewservice.MakeClerk(me, vshost)
    pb.finish = make(chan interface{})

    pb.currentView.Viewnum = 0
    pb.currentView.Primary = ""
    pb.currentView.Backup = ""
    pb.updateBackup = false

    pb.kvData = make(map[string]string)
    pb.preReply = make(map[int64]map[int64]string)

    rpcs := rpc.NewServer()
    rpcs.Register(pb)

    os.Remove(pb.me)
    l, e := net.Listen("unix", pb.me)
    if e != nil {
        log.Fatal("listen error: ", e)
    }
    pb.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    go func() {
        for pb.dead == false {
            conn, err := pb.l.Accept()
            if err == nil && pb.dead == false {
                if pb.unreliable && (rand.Int63()%1000) < 100 {
                    // discard the request.
                    conn.Close()
                } else if pb.unreliable && (rand.Int63()%1000) < 200 {
                    // process the request but force discard of reply.
                    c1 := conn.(*net.UnixConn)
                    f, _ := c1.File()
                    err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
                    if err != nil {
                        fmt.Printf("shutdown: %v\n", err)
                    }
                    pb.done.Add(1)
                    go func() {
                        rpcs.ServeConn(conn)
                        pb.done.Done()
                    }()
                } else {
                    pb.done.Add(1)
                    go func() {
                        rpcs.ServeConn(conn)
                        pb.done.Done()
                    }()
                }
            } else if err == nil {
                conn.Close()
            }
            if err != nil && pb.dead == false {
                fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
                pb.kill()
            }
        }
        DPrintf("%s: wait until all request are done\n", pb.me)
        pb.done.Wait()
        // If you have an additional thread in your solution, you could
        // have it read to the finish channel to hear when to terminate.
        close(pb.finish)
    }()

    pb.done.Add(1)
    go func() {
        for pb.dead == false {
            pb.tick()
            time.Sleep(viewservice.PingInterval)
        }
        pb.done.Done()
    }()

    return pb
}
