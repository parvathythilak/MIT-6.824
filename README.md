MIT-6.824
=========

My Solutions to course MIT-6.825 2014 Spring labs.

## Lab1 (MapReduce)

pkg:`main`, `mapreduce`

Because the project runs on Windows environment, I send RPCs via "TCP" rather than UNIX-domain sockets. So we should modify some code. On linux, we don't have to do the following.

In `woker.go`, close the `net.Listener` when the worker is shut down:

```go
func (wk *Worker) Shutdown(args *ShutdownArgs, res *ShutdownReply) error {
	DPrintf("Shutdown %s\n", wk.name)
	res.Njobs = wk.nJobs
	res.OK = true
	wk.nRPC = 1 // OK, because the same thread reads nRPC
	wk.nJobs--  // Don't count the shutdown RPC
	//** close the port
	wk.l.Close()
	return nil
}
```
In `test_test.go`, modify `port()` function to the real port.

```go
func port(suffix int) string {
	/*s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix*/
	p := suffix + 5101
	return ":" + strconv.Itoa(p)
}
```
When we use the function `call` in `common.go`, we should dial the right IP:
```
c, errx := rpc.Dial("tcp", "127.0.0.1"+srv)
```

run `go test` to get the result.

## Lab2 (Primary/Backup Key/Value Service)

pkg:`viewservice`, `pbservice`

Use a primary/backup replication for the key/value database, assisted by a view service that decides which machines are alive.

(Developing in Windows is like **, so move to Linux.)

## Lab3 (Paxos-based Key/Value Service)

pkg:`paxos`, `kvpaxos`

Use Paxos to manage the replication of a key/value store. 

## Lab4 (Sharded Key/Value Service)

pkg:`shardmaster`, `shardkv`

Build a key/value storage system that "shards," or partitions, the keys over a set of replica groups.

------




