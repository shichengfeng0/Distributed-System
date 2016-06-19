package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import (
	"os"
	"strconv"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	
	currentView View
	acked	bool
	idle	[]string
	pingKeeper map[string]time.Time
	viewMu sync.Mutex
}

func testLog(message string) {
	// Log the put operation into log file.
	f, err := os.OpenFile("TestLog.txt", os.O_APPEND|os.O_RDWR|os.O_CREATE , 0777)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	if _, err = f.WriteString(message + "\n"); err != nil {
		panic(err)
	}
}

func (vs *ViewServer) increaseViewNum() {
	testLog("IncreaseViewNum called *** New view, Primary: " + vs.currentView.Primary + ", Backup is " + vs.currentView.Backup)
	vs.mu.Lock()
	vs.currentView.Viewnum++
	vs.mu.Unlock()
}

func (vs *ViewServer) getViewNum() (result uint){
	vs.mu.Lock()
	result = vs.currentView.Viewnum
	vs.mu.Unlock()
	return
}

func convertView(view View) string {
	return "view# " + strconv.Itoa(int(view.Viewnum)) + ", primary: " + view.Primary + ", backup: " + view.Backup + ". "
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	
	vs.viewMu.Lock()

	if vs.getViewNum() == 0 {
		// If current view number in ping is 0

		// If ping view number is 0
		if args.Viewnum == 0 {
			// put the ping server into primary
			vs.currentView.Primary = args.Me
			vs.acked = false
			vs.increaseViewNum()
		} else {
			// If ping view number is greater than 0
			// ignore this case
		}
	} else {
		// If current view number in ping is greater than 0
		if vs.acked == false {
			// If acked is false

			if args.Me == vs.currentView.Primary {
				// If primary pings
				// If ping number equals to current view
				if args.Viewnum == vs.getViewNum() {
					// set acked = true
					vs.acked = true
					if vs.currentView.Backup == "" && len(vs.idle) > 0 {
						testLog("ACKED = False && PRIMARY -> Move Idle " + vs.idle[0] + "to backup")
						vs.currentView.Backup = vs.idle[0]
						vs.idle = vs.idle[1:]
					}
					testLog("ACKED = False && PRIMARY -> Current view: " + convertView(vs.currentView) + " is acked by primary" + args.Me)
					//					fmt.Println(vs.currentView, " acked by ", vs.currentView.Primary)
				} else {
					// If ping number less than current view(including the case when primary restarts)
					// ignore this case
					testLog("ACKED = False && PRIMARY -> WRONG CASE: Primary ping view number " + strconv.Itoa(int(args.Viewnum)) + "--> Current viewnum is " + strconv.Itoa(int(vs.currentView.Viewnum)))
//					fmt.Println("CAODAN ERHAO *(*(*(*(*(*(*(*(")
				}
			} else if args.Me == vs.currentView.Backup {
				// If backup pings
				// Do nothing.
				testLog("ACKED = False && BACKUP -> " + args.Me + " pinged view " + strconv.Itoa(int(args.Viewnum)))
				//				fmt.Println("TEST #4: ", vs.currentView)
			} else {
				// If others pings
				// put it into idle
				if !idleContains(vs.idle, args.Me) {
					testLog("ACKED = False && OTHERS -> " + args.Me + " added to idle")
					vs.idle = append(vs.idle, args.Me)
				}
				testLog("ACKED = False && OTHERS -> " + args.Me + " pinged view " + strconv.Itoa(int(args.Viewnum)))
			}
		} else {
			// If acked is true
			if args.Me == vs.currentView.Primary { // If primary pings
				// If ping number equals to current view
					// doing nothing
				// If ping number less than current view but more than 0
					// Do nothing
				if args.Viewnum == 0 {
					// If ping number is 0 (Primary restarts)
					testLog("ACKED = True && PRIMARY -> " + args.Me + " pinged view " + strconv.Itoa(int(args.Viewnum)) + " Primary RESTARTS!!!")
					testLog("ACKED = True && PRIMARY (View before changed) " + convertView(vs.currentView))

					if vs.currentView.Backup != "" {
						// If has backup
						// promote backup and put the old primary into idle

						vs.currentView.Primary = vs.currentView.Backup
						vs.currentView.Backup = args.Me

						// Change ack
						vs.acked = false

						// Increase view number
						vs.increaseViewNum()
//						fmt.Println("TEST #5: ", vs.currentView)
						testLog("ACKED = True to False && PRIMARY (View after changed) " + convertView(vs.currentView))
					} else {
						testLog("ACKED = True && PRIMARY is STUCK " + args.Me + " pinged " + strconv.Itoa(int(args.Viewnum)))
						// If no backup
						// STUCK!!
					}
				}
			} else if args.Me == vs.currentView.Backup {
				// If backup pings

				if args.Viewnum == 0 {
					// If ping nunmber is 0(restart)
					// Set backup to "", put old backup to idle, set ack = false
					testLog("ACKED = True && BACKUP -> " + args.Me + " pinged view " + strconv.Itoa(int(args.Viewnum)) + " Backup RESTARTS!!!")
					vs.currentView.Backup = ""
					if !idleContains(vs.idle, args.Me) {
						vs.idle = append(vs.idle, args.Me)
					}
					vs.acked = false
					// Increase view number
					vs.increaseViewNum()
					testLog("ACKED = True to False && BACKUP -> Current view is " + convertView(vs.currentView))
					//					fmt.Println("TEST #6: ", vs.currentView)
				}
			} else {
				// If others pings
				testLog("ACKED = True && OTHERS -> " + args.Me + " pinged " + strconv.Itoa(int(args.Viewnum)))
				if vs.currentView.Backup == "" {
					// If no backup
					// put ping server into backup and set acked = false
//					fmt.Println("Before backup added: ", vs.currentView)
//					fmt.Println("Add backup: ", args.Me)
					if (idleContains(vs.idle, args.Me)) {
						vs.currentView.Backup = vs.idle[0]
						vs.idle = vs.idle[1:]
					} else {
						vs.currentView.Backup = args.Me
					}
					vs.acked = false

					// Increase view number
					vs.increaseViewNum()
					testLog("ACKED = True to False && OTHERS -> View after change " + convertView(vs.currentView))
					//					fmt.Println("After backup added: ", vs.currentView)
				} else {
					// If already has backup
					// put it into idle
//					fmt.Println("TEST #600: put", args.Me, " in idle")
					if !idleContains(vs.idle, args.Me) {
						vs.idle = append(vs.idle, args.Me)
					}
				}
			}
		}
	}

	// Set up the return view
	vs.pingKeeper[args.Me] = time.Now()
	reply.View = vs.currentView
	testLog("Returning view" + convertView(vs.currentView))
	vs.viewMu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	
	vs.viewMu.Lock()
	reply.View = vs.currentView
	vs.viewMu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	
	vs.viewMu.Lock()

	// Primary !== ""
	// See if any server died
	for k, v :=  range vs.pingKeeper{
		server := k
		difference := time.Since(v)
		if difference > PingInterval * DeadPings {
			switch server {
			case vs.currentView.Primary:
				testLog("TICKED PRIMARY DIED " + convertView(vs.currentView))
				// Primary died
				// Check if acked is true
//				fmt.Println("Primary: ", vs.currentView.Primary, " died")
				if vs.acked {
					// Check if backup is available
					if vs.currentView.Backup != "" {
//						fmt.Println("Put backup: ", vs.currentView.Backup, " in")
						// Turn backup into primary
						vs.currentView.Primary = vs.currentView.Backup
						vs.currentView.Backup = ""

						// Turn idle into backup
						if len(vs.idle) > 0 {
//							fmt.Println("TEST #150: ", vs.currentView)
							vs.currentView.Backup = vs.idle[0]
//							fmt.Println("TEST #151: ", vs.currentView)
							vs.idle = vs.idle[1:]
						}

						vs.acked = false
						vs.pingKeeper[k] = time.Now()
						vs.increaseViewNum();
						testLog("ACKED = TRUE && PRIMARY DIED -> New view is " + convertView(vs.currentView))
					}
//					fmt.Println("TEST #1: ", vs.currentView)
				} else {
					// crash!!!!
				}

			case vs.currentView.Backup:
				// Backup died
				// Check if acked is true
//				fmt.Println("Backup: ", vs.currentView.Backup, " died")
				testLog("TICKED BACKUP DIED " + convertView(vs.currentView))
				if vs.acked {
//					fmt.Println("TEST #180: ", vs.currentView)
					vs.currentView.Backup = ""
					if len(vs.idle) > 0 {
						vs.currentView.Backup = vs.idle[0]
//						fmt.Println("TEST #2: ", vs.currentView)
						vs.idle = vs.idle[1:]
					}
					vs.acked = false
					vs.increaseViewNum();
					testLog("ACKED = TRUE && BACKUP DIED -> New view is " + convertView(vs.currentView))
				} else {
					// crash!!!!
				}
			default:
				// Idle died
				// Delete from idle
				for i, idleServer := range vs.idle {
					if server == idleServer {
						vs.idle = append(vs.idle[0:i], vs.idle[i+1:]...)
					}
				}
			}
		}
	}

	vs.viewMu.Unlock()
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
	// Your vs.* initializations here.
	vs.idle = make([]string, 0)
	vs.currentView = View{0, "", ""}
	vs.acked = false
	vs.pingKeeper = make(map[string]time.Time)

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

func idleContains(idle []string, target string) bool {
	for  _, server := range idle {
		if server == target {
			return true
		}
	}
	return false
}