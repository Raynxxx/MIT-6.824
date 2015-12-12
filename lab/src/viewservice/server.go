package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	view        View
	primaryACK  uint
	backupACK   uint
	currentTick uint
	primaryTick uint
	backupTick  uint
}

//
// ACKed by primary of current view
//
func (vs *ViewServer) ACKed() bool {
	return vs.view.Viewnum == vs.primaryACK
}

//
// viewserver judge If has primary server
//
func (vs *ViewServer) HasPrimary() bool {
	return vs.view.Primary != ""
}

//
// viewserver judge If has backup server
//
func (vs *ViewServer) HasBackup() bool {
	return vs.view.Backup != ""
}

//
// viewserver judge the name is primary server
//
func (vs *ViewServer) isPrimary(name string) bool {
	return vs.view.Primary == name
}

//
// viewserver judge the name is backup server
//
func (vs *ViewServer) isBackup(name string) bool {
	return vs.view.Backup == name
}

//
// Backup takes over if primary fails
//
func (vs *ViewServer) promoteBackup() {
	if !vs.HasBackup() {
		return
	}
	vs.view.Primary = vs.view.Backup
	vs.view.Backup = ""
	vs.view.Viewnum++
	vs.primaryACK = vs.backupACK
	vs.primaryTick = vs.backupTick
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	viewnum := args.Viewnum
	name := args.Me
	vs.mu.Lock()

	switch {

	case !vs.HasPrimary() && vs.view.Viewnum == 0:
		vs.view.Primary = name
		vs.view.Viewnum = viewnum + 1
		vs.primaryACK = 0
		vs.primaryTick = vs.currentTick

	case vs.isPrimary(name):
		if viewnum == 0 {
			vs.promoteBackup()
		} else {
			vs.primaryACK = viewnum
			vs.primaryTick = vs.currentTick
		}

	case !vs.HasBackup() && vs.ACKed():
		vs.view.Backup = name
		vs.view.Viewnum++
		vs.backupTick = vs.currentTick

	case vs.isBackup(name):
		if viewnum == 0 && vs.ACKed() {
			vs.view.Backup = name
			vs.view.Viewnum++
		}
		vs.backupTick = vs.currentTick

	}
	reply.View = vs.view
	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	vs.currentTick++
	if vs.currentTick-vs.primaryTick >= DeadPings && vs.ACKed() {
		vs.promoteBackup()
	}
	if vs.HasBackup() && vs.currentTick-vs.backupTick >= DeadPings && vs.ACKed() {
		vs.view.Backup = ""
		vs.view.Viewnum++
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{0, "", ""}
	vs.primaryACK = 0
	vs.backupACK = 0
	vs.currentTick = 0
	vs.primaryTick = 0
	vs.backupTick = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
