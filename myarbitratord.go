/*
  Copyright 2017 Matthew Lord (mattalord@gmail.com)

  WARNING: This is experimental and for demonstration purposes only!

  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

   3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
	// uncomment the next import to add profiling to the binary, available via "/debug/pprof" in the RESTful API
	//_ "net/http/pprof"
	//"github.com/mattlord/myarbitratord/replication/group"
	"./replication/group"
)

type MembersByOnlineNodes []group.Node

var debug = false

var InfoLog = log.New(os.Stderr,
	"INFO: ",
	log.Ldate|log.Ltime|log.Lshortfile)

var DebugLog = log.New(os.Stderr,
	"DEBUG: ",
	log.Ldate|log.Ltime|log.Lshortfile)

// This is where I'll store all operating status metrics, presented as JSON via the "/stats" HTTP API call
type stats struct {
	StartTime   string       `json:"Started"`
	Uptime      string       `json:"Uptime"`
	Loops       uint         `json:"Loops"`
	Partitions  uint         `json:"Partitions"`
	CurrentSeed group.Node   `json:"Current Seed Node"`
	LastView    []group.Node `json:"Last Membership View"`
	sync.RWMutex
}

var mystats = stats{StartTime: time.Now().Format(time.RFC1123), Loops: 0, Partitions: 0}

// This will simply note the available API calls
func defaultHandler(httpW http.ResponseWriter, httpR *http.Request) {
	if debug {
		DebugLog.Println("Handling HTTP request without API call.")
	}

	fmt.Fprintf(httpW, "Welcome to the MySQL Arbitrator's RESTful API handler!\n\nThe available API calls are:\n/stats: Provide runtime and operational stats\n")
}

// This will serve the stats via a simple RESTful API
func statsHandler(httpW http.ResponseWriter, httpR *http.Request) {
	mystats.RLock()

	if debug {
		DebugLog.Printf("Handling HTTP request for stats.")
	}

	tval, terr := time.Parse(time.RFC1123, mystats.StartTime)
	if terr != nil {
		InfoLog.Printf("Error parsing time value for stats: %+v\n", terr)
	}
	dval := time.Since(tval)

	mystats.RUnlock()
	mystats.Lock()
	mystats.Uptime = dval.String()
	mystats.Unlock()
	mystats.RLock()

	statsJSON, err := json.MarshalIndent(&mystats, "", "    ")

	if err != nil {
		InfoLog.Printf("Error handling HTTP request for stats: %+v\n", err)
	}

	fmt.Fprintf(httpW, "%s", statsJSON)

	mystats.RUnlock()
}

func main() {
	var SeedHost string
	var SeedPort string
	var MysqlUser string
	var MysqlPass string
	var MysqlAuthFile string
	type json_mysql_auth struct {
		User     string `json:"user"`
		Password string `json:"password"`
	}

	http.DefaultServeMux.HandleFunc("/", defaultHandler)
	http.DefaultServeMux.HandleFunc("/stats", statsHandler)
	var HttpPort string

	flag.StringVar(&SeedHost, "seed-host", "", "IP/Hostname of the seed node used to start monitoring the Group Replication cluster (Required Parameter!)")
	flag.StringVar(&SeedPort, "seed-port", "3306", "Port of the seed node used to start monitoring the Group Replication cluster")
	flag.BoolVar(&debug, "debug", false, "Execute in debug mode with all debug logging enabled")
	flag.StringVar(&MysqlUser, "mysql-user", "root", "The mysql user account to be used when connecting to any node in the cluster")
	flag.StringVar(&MysqlPass, "mysql-password", "", "The mysql user account password to be used when connecting to any node in the cluster")
	flag.StringVar(&MysqlAuthFile, "mysql-auth-file", "", "The JSON encoded file containining user and password entities for the mysql account to be used when connecting to any node in the cluster")
	flag.StringVar(&HttpPort, "http-port", "8099", "The HTTP port used for the RESTful API")

	flag.Parse()

	// ToDo: I need to handle the password on the command-line more securely
	//       I need to do some data masking for the processlist

	// A host is required, the default port of 3306 will then be attempted
	if SeedHost == "" {
		fmt.Fprintf(os.Stderr, "No value specified for required flag: -SeedHost\n")
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	// let's start a thread to handle the RESTful API calls
	InfoLog.Printf("Starting HTTP server for RESTful API on port %s\n", HttpPort)
	go http.ListenAndServe(":"+HttpPort, http.DefaultServeMux)

	if debug {
		group.Debug = true
	}

	if MysqlAuthFile != "" && MysqlPass == "" {
		if debug {
			DebugLog.Printf("Reading MySQL credentials from file: %s\n", MysqlAuthFile)
		}

		jsonfile, err := ioutil.ReadFile(MysqlAuthFile)

		if err != nil {
			log.Fatal("Could not read mysql credentials from specified file: " + MysqlAuthFile)
		}

		var jsonauth json_mysql_auth
		json.Unmarshal(jsonfile, &jsonauth)

		if debug {
			DebugLog.Printf("Unmarshaled mysql auth file contents: %+v\n", jsonauth)
		}

		MysqlUser = jsonauth.User
		MysqlPass = jsonauth.Password

		if MysqlUser == "" || MysqlPass == "" {
			errstr := "Failed to read user and password from " + MysqlAuthFile + ". Ensure that the file contents are in the required format: \n{\n  \"user\": \"myser\",\n  \"password\": \"mypass\"\n}"
			log.Fatal(errstr)
		}

		if debug {
			DebugLog.Printf("Read mysql auth info from file. user: %s, password: %s\n", MysqlUser, MysqlPass)
		}
	}

	InfoLog.Println("Welcome to the MySQL Group Replication Arbitrator!")

	InfoLog.Printf("Starting operations from seed node: '%s:%s'\n", SeedHost, SeedPort)
	SeedNode := group.New(SeedHost, SeedPort, MysqlUser, MysqlPass)
	err := MonitorCluster(*SeedNode)

	if err != nil {
		log.Fatal(err)
		os.Exit(100)
	} else {
		os.Exit(0)
	}
}

func MonitorCluster(SeedNode group.Node) error {
	loop := true
	var err error
	LastView := []group.Node{}

	for loop == true {
		mystats.Lock()
		mystats.Loops = mystats.Loops + 1
		mystats.CurrentSeed = SeedNode
		// Setting the slice to nil will clear it and properly release all of the previous contents for the GC
		mystats.LastView = nil
		mystats.LastView = LastView
		mystats.Unlock()

		// let's check the status of the current seed node
		err = SeedNode.Connect()
		defer SeedNode.Cleanup()

		if err != nil || SeedNode.MemberState != "ONLINE" {
			// if we couldn't connect to the current seed node or it's no longer part of the group
			// let's try and get a new seed node from the last known membership view
			InfoLog.Println("Attempting to get a new seed node...")

			for i := 0; i < len(LastView); i++ {
				if SeedNode != LastView[i] {
					err = LastView[i].Connect()
					defer LastView[i].Cleanup()

					if err == nil && LastView[i].MemberState == "ONLINE" {
						SeedNode = LastView[i]
						InfoLog.Printf("Updated seed node! New seed node is: '%s:%s'\n", SeedNode.MysqlHost, SeedNode.MysqlPort)
						break
					}
				}

				LastView[i].Cleanup()
			}
		}

		// If we still don't have a valid seed node...
		if err != nil || SeedNode.MemberState != "ONLINE" {
			// if we already have a valid list of nodes to re-try, then let's "reset" it before we loop again
			if len(LastView) > 0 {
				SeedNode.Reset()
			}
			time.Sleep(time.Millisecond * 1000)
			continue
		}

		members, err := SeedNode.GetMembers()

		if err != nil || SeedNode.OnlineParticipants < 1 {
			// Something is still fishy with our seed node
			// if we already have a valid list of nodes to re-try, then let's "reset" it before we loop again
			if len(LastView) > 0 {
				SeedNode.Reset()
			}
			time.Sleep(time.Millisecond * 1000)
			continue
		}

		quorum, err := SeedNode.HasQuorum()

		if err != nil {
			// Something is still fishy with our seed node
			// if we already have a valid list of nodes to re-try, then let's "reset" it before we loop again
			if len(LastView) > 0 {
				SeedNode.Reset()
			}
			time.Sleep(time.Millisecond * 1000)
			continue
		}

		if debug {
			DebugLog.Printf("Seed node details: %+v", SeedNode)
		}

		if quorum {
			// Let's see if there are any nodes that are no longer fully functioning members of the group and then take action

			for i := 0; i < len(LastView); i++ {
				if SeedNode != LastView[i] {
					err = LastView[i].Connect()
					defer LastView[i].Cleanup()

					if err == nil {
						// If Group Replication has been stopped, then let's set super_read_only mode to protect consistency
						// But not shut it down, as the DBA may need to perform some maintenance
						if LastView[i].MemberState == "OFFLINE" {
							InfoLog.Printf("Enabling read only mode on OFFLINE node: '%s:%s'\n", LastView[i].MysqlHost, LastView[i].MysqlPort)

							LastView[i].SetReadOnly(true)
						} else {
							quorum, err = LastView[i].HasQuorum()

							// If this node sees itself in the ERROR state or doesn't think it has a quorum, then it should be safe to shut it down
							if LastView[i].MemberState == "ERROR" || quorum == false {
								InfoLog.Printf("Shutting down non-healthy node: '%s:%s'\n", LastView[i].MysqlHost, LastView[i].MysqlPort)
								err = LastView[i].Shutdown()
							}
						} // if we couldn't connect, then not much we can do...
					}

					LastView[i].Cleanup()
				}
			}
		} else {
			// handling other network partitions and split brain scenarios will be much trickier... I'll need to try and
			// contact each member in the last seen view and try to determine which partition should become the
			// primary one. We'll then need to contact 1 node in the new primary partition and explicitly set the new
			// membership with 'set global group_replication_force_members="<node_list>"'. Finally we'll need to try
			// and connect to the nodes on the losing side(s) of the partition and attempt to shutdown the mysqlds

			InfoLog.Println("Network partition detected! Attempting to handle... ")
			mystats.Lock()
			mystats.Partitions = mystats.Partitions + 1
			mystats.Unlock()

			// does anyone have a quorum? Let's double check before forcing the membership
			PrimaryPartition := false

			for i := 0; i < len(LastView); i++ {
				var err error

				err = LastView[i].Connect()
				defer LastView[i].Cleanup()

				if err == nil {
					quorum, err = LastView[i].HasQuorum()
					// let's make sure that the OnlineParticipants is up to date
					_, err = LastView[i].GetMembers()
				}

				if err == nil && quorum {
					SeedNode = LastView[i]
					PrimaryPartition = true
					break
				}

				LastView[i].Cleanup()
			}

			// If no one in fact has a quorum, then let's see which partition has the most
			// online/participating/communicating members. The participants in that partition
			// will then be the ones that we use to force the new membership and unlock the cluster
			if PrimaryPartition == false && len(LastView) > 0 {
				InfoLog.Println("No primary partition found! Attempting to choose and force a new one ... ")

				sort.Sort(MembersByOnlineNodes(LastView))

				if debug {
					DebugLog.Printf("Member view sorted by number of online nodes: %+v\n", LastView)
				}

				// now the last element in the array is the one to use as it's coordinating with the most nodes
				ViewLen := len(LastView) - 1
				SeedNode = LastView[ViewLen]

				// *BUT*, if there's no clear winner based on sub-partition size, then we should pick the sub-partition (which
				// can be 1 node) that has executed the most GTIDs
				if ViewLen >= 1 && LastView[ViewLen].OnlineParticipants == LastView[ViewLen-1].OnlineParticipants {
					bestmemberpos := ViewLen
					var bestmembertrxcnt uint64 = 0
					var curtrxcnt uint64 = 0
					bestmembertrxcnt, err = LastView[ViewLen].TransactionsExecutedCount()

					// let's loop backwards through the array as it's sorted by online participants / partition size now
					// skipping the last one as we already have the info for it
					for i := ViewLen - 1; i >= 0; i-- {
						if LastView[i].OnlineParticipants == LastView[bestmemberpos].OnlineParticipants {
							curtrxcnt, err = LastView[i].TransactionsExecutedCount()

							if curtrxcnt > bestmembertrxcnt {
								bestmembertrxcnt = curtrxcnt
								bestmemberpos = i
							}
						} else {
							// otherwise we've gone backwards far enough and we have the best option
							break
						}
					}

					SeedNode = LastView[bestmemberpos]
				}

				err = SeedNode.Connect()
				defer SeedNode.Cleanup()

				if err != nil {
					// seed node is no good
					// if we already have a valid list of nodes to re-try, then let's "reset" it before we loop again
					if len(LastView) > 0 {
						SeedNode.Reset()
					}
					time.Sleep(time.Millisecond * 1000)
					continue
				}

				// let's build a string of '<host>:<port>' combinations that we want to use for the new membership view
				members, _ := SeedNode.GetMembers()

				ForceMemberString := ""
				var MemberGcsAddr string

				for _, member := range members {
					err = member.Connect()
					defer member.Cleanup()

					if err == nil && member.MemberState == "ONLINE" {
						if ForceMemberString != "" {
							ForceMemberString = ForceMemberString + ","
						}

						// we need to get the GCS/XCom 'host:port' combination, which is different from the 'host:port' combination for mysqld
						MemberGcsAddr, err = member.GetGCSAddress()

						if err == nil {
							ForceMemberString = ForceMemberString + MemberGcsAddr
						} else {
							InfoLog.Printf("Problem getting GCS endpoint for '%s:%s': %+v\n", member.MysqlHost, member.MysqlPort, err)
						}
					} else {
						member.MemberState = "SHOOT_ME"
					}

					member.Cleanup()
				}

				if ForceMemberString != "" {
					InfoLog.Printf("Forcing group membership to form new primary partition! Using: '%s'\n", ForceMemberString)

					err := SeedNode.ForceMembers(ForceMemberString)

					if err != nil {
						InfoLog.Printf("Error forcing group membership: %v\n", err)
					} else {
						// We successfully unblocked the group, now let's try and politely STONITH the nodes in the losing partition
						for _, member := range members {
							if member.MemberState == "SHOOT_ME" {
								err = member.Shutdown()
							}

							if err != nil {
								InfoLog.Printf("Could not shutdown node: '%s:%s'\n", member.MysqlHost, member.MysqlPort)
							}
						}
					}
				} else {
					InfoLog.Println("No valid group membership to force!")
				}
			}
		}

		// Setting the slice to nil will clear it and properly release all of the previous contents for the GC
		LastView = nil
		// Let's now save a copy of latest view in case the seed node is no longer valid next time
		LastView = make([]group.Node, len(members))
		copy(LastView, members)

		// let's force garbage collection while we sleep
		go runtime.GC()
		time.Sleep(time.Millisecond * 2000)
	}

	return err
}

// The remaining functions are used to sort our membership slice
// We'll never have a super high number of nodes involved, so a simple bubble sort will suffice
func (a MembersByOnlineNodes) Len() int {
	return len(a)
}

func (a MembersByOnlineNodes) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a MembersByOnlineNodes) Less(i, j int) bool {
	return a[i].OnlineParticipants < a[j].OnlineParticipants
}
