package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
    mu   sync.Mutex
    l    net.Listener
    dead bool
    me   string

    currentView    View
    recentPingTime map[string]time.Time
    isPrimaryACK   bool
    aliveServers   map[string]bool
    updatedServers map[string]bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

    _, isExist := vs.recentPingTime[args.Me]

    if isExist {
        vs.recentPingTime[args.Me] = time.Now()
        vs.aliveServers[args.Me] = true
        if args.Viewnum == vs.currentView.Viewnum {
            // mark this server updated, i.e. it knows the current correct view
            vs.updatedServers[args.Me] = true
        } else {
            vs.updatedServers[args.Me] = false
        }
        if vs.isPrimaryACK {
            if args.Me == vs.currentView.Primary {
                if args.Viewnum != vs.currentView.Viewnum {
                    // primary crashed and restarted
                    vs.aliveServers[args.Me] = false
                    delete(vs.recentPingTime, args.Me)
                }
            }
        } else {
            // primary hasn't ack
            if (args.Me == vs.currentView.Primary) && (args.Viewnum == vs.currentView.Viewnum) {
                vs.isPrimaryACK = true
            }
        }
    } else {
        // new server
        // but make it alive next ping
        // or it will be complicated to update view
        vs.recentPingTime[args.Me] = time.Now()
    }

    reply.View = vs.currentView

    return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

    reply.View = vs.currentView
    return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
    for server, lastPing := range vs.recentPingTime {
        if time.Since(lastPing).Nanoseconds() >= PingInterval.Nanoseconds()*DeadPings {
            // mark server dead
            vs.aliveServers[server] = false
            vs.updatedServers[server] = false
            delete(vs.recentPingTime, server)
        }
    }

    // update view
    shouldIncreaseViewNum := false
    p, pExist := vs.aliveServers[vs.currentView.Primary]
    b, bExist := vs.aliveServers[vs.currentView.Backup]
    shouldAssignNewPrimary := vs.isPrimaryACK && (!pExist || (pExist && !p))
    shouldAssignNewBackup := vs.isPrimaryACK && (!bExist || (bExist && !b))

    if shouldAssignNewPrimary {
        if (bExist && b) && (vs.updatedServers[vs.currentView.Backup]) {
            // promote the backup to primary
            log.Println("promote the backup to primary")
            vs.currentView.Primary = vs.currentView.Backup
            vs.currentView.Backup = ""
            shouldAssignNewBackup = true
            shouldIncreaseViewNum = true
            vs.isPrimaryACK = false
        } else {
            // backup is not avaliabe, choose an updated server instead
            for server, isUpdated := range vs.updatedServers {
                if !isUpdated {
                    continue
                }
                if (server != vs.currentView.Primary) && (server != vs.currentView.Backup) {
                    log.Println("choose primary: ", server)
                    vs.currentView.Primary = server
                    vs.isPrimaryACK = false
                    shouldIncreaseViewNum = true
                    break
                }
            }
        }
    }

    if shouldAssignNewBackup {
        for server, isAvaliabe := range vs.aliveServers {
            if !isAvaliabe {
                continue
            }
            if (server != vs.currentView.Primary) && (server != vs.currentView.Backup) {
                vs.currentView.Backup = server
                vs.isPrimaryACK = false
                shouldIncreaseViewNum = true
                break
            }
        }
    }

    if shouldIncreaseViewNum {
        vs.currentView.Viewnum++
        log.Println("increase view num to ", vs.currentView.Viewnum)
    }

}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
    vs.dead = true
    vs.l.Close()
}

func StartServer(me string) *ViewServer {
    vs := new(ViewServer)
    vs.me = me

    vs.currentView.Backup = ""
    vs.currentView.Primary = ""
    vs.currentView.Viewnum = 0
    vs.isPrimaryACK = true

    vs.aliveServers = make(map[string]bool)
    vs.recentPingTime = make(map[string]time.Time)
    vs.updatedServers = make(map[string]bool)

    // tell net/rpc about our RPC server and handlers.
    rpcs := rpc.NewServer()
    rpcs.Register(vs)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(vs.me) // only needed for "unix"
    //l, e := net.Listen("unix", vs.me)
    l, e := net.Listen("tcp", vs.me)
    if e != nil {
        log.Fatal("listen error: ", e)
    }
    vs.l = l

    // please don't change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections from clients.
    go func() {
        for vs.dead == false {
            conn, err := vs.l.Accept()
            if err == nil && vs.dead == false {
                go rpcs.ServeConn(conn)
            } else if err == nil {
                conn.Close()
            }
            if err != nil && vs.dead == false {
                fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
                vs.Kill()
            }
        }
    }()

    // create a thread to call tick() periodically.
    go func() {
        for vs.dead == false {
            vs.tick()
            time.Sleep(PingInterval)
        }
    }()

    return vs
}
