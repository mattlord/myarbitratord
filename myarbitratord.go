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
  "os"
  "log"
  "time"
  "flag"
  "sort"
  "fmt"
  "net/http"
  "io/ioutil"
  "encoding/json"
  "github.com/mattlord/myarbitratord/group_replication/instances"
)

type MembersByOnlineNodes []instances.Instance
var debug = false

var InfoLog = log.New( os.Stderr,
              "INFO: ",
              log.Ldate|log.Ltime|log.Lshortfile )

var DebugLog = log.New( os.Stderr,
               "DEBUG: ",
               log.Ldate|log.Ltime|log.Lshortfile )

// This is where I'll store all operating status metrics, presented as JSON via HTTP 
type stats struct {
  // This will really be the process start time that I can then use to display the uptime 
  Start_time string	`json:"Started"`
  Uptime string		`json:"Uptime"`
  Loops  uint		`json:"Loops"`
  Partitions uint	`json:"Partitions"`
}
var mystats = stats{ Start_time: time.Now().String(), Loops: 0, Partitions: 0 }

// This will simply note the available API calls
func defaultHandler( httpW http.ResponseWriter, httpR *http.Request ){
  if( debug ){
    DebugLog.Println( "Handling HTTP request without API call." )
  }

  fmt.Fprintf( httpW, "Welcome to the MySQL Arbitrator's RESTful API handler!\n\nThe available API calls are:\n/stats: Provide runtime and operational stats\n" )
}

// This will serve the stats via a simple RESTful API
func statsHandler( httpW http.ResponseWriter, httpR *http.Request ){
  if( debug ){
    DebugLog.Printf( "Handling HTTP request for stats. Current stats are: %+v\n", mystats )
  }

  tval, _ := time.Parse( mystats.Start_time, mystats.Start_time )
  mystats.Uptime = tval.String()

  statsJSON, err := json.Marshal( &mystats )

  if( err != nil ){
    InfoLog.Printf( "Error handling HTTP request for stats: %+v\n", err )
  }
  
  fmt.Fprintf( httpW, "%s", statsJSON )
}


func main(){
  var seed_host string 
  var seed_port string 
  var mysql_user string
  var mysql_pass string
  var mysql_auth_file string
  type json_mysql_auth struct {
    User string      `json:"user"`
    Password string  `json:"password"`
  }
  // let's start a thread to handle the RESTful API calls
  serverMux := http.NewServeMux()
  serverMux.HandleFunc( "/", defaultHandler )
  serverMux.HandleFunc( "/stats", statsHandler )
  var http_port string = "8099"

  flag.StringVar( &seed_host, "seed_host", "", "IP/Hostname of the seed node used to start monitoring the Group Replication cluster (Required Parameter!)" )
  flag.StringVar( &seed_port, "seed_port", "3306", "Port of the seed node used to start monitoring the Group Replication cluster" )
  flag.BoolVar( &debug, "debug", false, "Execute in debug mode with all debug logging enabled" )
  flag.StringVar( &mysql_user, "mysql_user", "root", "The mysql user account to be used when connecting to any node in the cluster" )
  flag.StringVar( &mysql_pass, "mysql_password", "", "The mysql user account password to be used when connecting to any node in the cluster" )
  flag.StringVar( &mysql_auth_file, "mysql_auth_file", "", "The JSON encoded file containining user and password entities for the mysql account to be used when connecting to any node in the cluster" )
  flag.StringVar( &http_port, "http_port", "8099", "The HTTP port used for the RESTful stats API" )

  flag.Parse()

  // ToDo: I need to handle the password on the command-line more securely
  //       I need to do some data masking for the processlist 

  // A host is required, the default port of 3306 will then be attempted 
  if( seed_host == "" ){
    fmt.Fprintf( os.Stderr, "No value specified for required flag: -seed_host\n" )
    fmt.Fprintf( os.Stderr, "Usage of %s:\n", os.Args[0] )
    flag.PrintDefaults()
    os.Exit( 1 )
  }

  InfoLog.Printf( "Starting HTTP server for RESTful stats information on port %s\n", http_port )
  go http.ListenAndServe( ":" + http_port, serverMux )

  if( debug ){
    instances.Debug = true
  }

  if( mysql_auth_file != "" && mysql_pass == "" ){
    if( debug ){
      DebugLog.Printf( "Reading MySQL credentials from file: %s\n", mysql_auth_file )
    }

    jsonfile, err := ioutil.ReadFile( mysql_auth_file )

    if( err != nil ){
      log.Fatal( "Could not read mysql credentials from specified file: " + mysql_auth_file )
    }

    var jsonauth json_mysql_auth
    json.Unmarshal( jsonfile, &jsonauth )

    if( debug ){
      DebugLog.Printf( "Unmarshaled mysql auth file contents: %+v\n", jsonauth )
    }

    mysql_user = jsonauth.User
    mysql_pass = jsonauth.Password

    if( mysql_user == "" || mysql_pass == "" ){
      errstr := "Failed to read user and password from " + mysql_auth_file + ". Ensure that the file contents are in the required format: \n{\n  \"user\": \"myser\",\n  \"password\": \"mypass\"\n}"
      log.Fatal( errstr )
    }
  
    if( debug ){
      DebugLog.Printf( "Read mysql auth info from file. user: %s, password: %s\n", mysql_user, mysql_pass )
    }
  }

  InfoLog.Println( "Welcome to the MySQL Group Replication Arbitrator!" )

  InfoLog.Printf( "Starting operations from seed node: '%s:%s'\n", seed_host, seed_port )

  seed_node := instances.New( seed_host, seed_port, mysql_user, mysql_pass )
  err := MonitorCluster( seed_node )
  
  if( err != nil ){
    log.Fatal( err )
    os.Exit( 100 )
  } else {
    os.Exit( 0 )
  }
}


func MonitorCluster( seed_node *instances.Instance ) error {
  loop := true
  var err error
  last_view := []instances.Instance{}
  
  for( loop == true ){
    mystats.Loops = mystats.Loops + 1

    // let's check the status of the current seed node
    // if the seed node 
    err = seed_node.Connect()
  
    if( err != nil || seed_node.Member_state != "ONLINE" ){
      // if we couldn't connect to the current seed node or it's no longer part of the group
      // let's try and get a new seed node from the last known membership view 
      InfoLog.Println( "Attempting to get a new seed node..." )

      for i := 0; i < len(last_view); i++ {
        err = last_view[i].Connect()
        if( err == nil && last_view[i].Member_state == "ONLINE" ){
          seed_node = &last_view[i]
          InfoLog.Printf( "Updated seed node! New seed node is: '%s:%s'\n", seed_node.Mysql_host, seed_node.Mysql_port ) 
          break
        }
      }
    }

    members, err := seed_node.GetMembers()

    if( err != nil ){
      // something is up with our current seed node, let's loop again 
      continue
    }

    // save this view in case the seed node is no longer valid next time 
    //last_view = copy( last_view, members[:] )
    last_view = *members

    quorum, err := seed_node.HasQuorum()

    if( err != nil ){
      // something is up with our current seed node, let's loop again 
      continue
    }

    if( debug ){
      DebugLog.Printf( "Seed node details: %+v", seed_node )
    } 

    if( quorum ){
      // Let's try and shutdown the nodes NOT in the primary partition if we can reach them from the arbitrator 

      for _, member := range *members {
        if( member.Member_state == "ERROR" || member.Member_state == "UNREACHABLE" ){
          InfoLog.Printf( "Shutting down non-healthy node: '%s:%s'\n", member.Mysql_host, member.Mysql_port )
          
          err = member.Connect()

          if( err != nil ){
            InfoLog.Printf( "Could not connect to '%s:%s' in order to shut it down\n", member.Mysql_host, member.Mysql_port )
          } else {
            err = member.Shutdown()
          }

          if( err != nil ){
            InfoLog.Printf( "Could not shutdown instance: '%s:%s'\n", member.Mysql_host, member.Mysql_port )
          }
        } 
      }
    } else {
      // handling other network partitions and split brain scenarios will be much trickier... I'll need to try and
      // contact each member in the last seen view and try to determine which partition should become the
      // primary one. We'll then need to contact 1 node in the new primary partition and explicitly set the new
      // membership with 'set global group_replication_force_members="<node_list>"'. Finally we'll need to try
      // and connect to the nodes on the losing side(s) of the partition and attempt to shutdown the mysqlds

      InfoLog.Println( "Network partition detected! Attempting to handle... " )

      // does anyone have a quorum? Let's double check before forcing the membership 
      primary_partition := false

      for i := 0; i < len(last_view); i++ {
        var err error 
       
        err = last_view[i].Connect()    
      
        if( err == nil ){
          quorum, err = last_view[i].HasQuorum()
          // let's make sure that the Online_participants is up to date 
          _, err = last_view[i].GetMembers()
        }

        if( err == nil && quorum ){
          seed_node = &last_view[i]
          primary_partition = true
          break
        }
      }

      // If no one in fact has a quorum, then let's see which partition has the most
      // online/participating/communicating members. The participants in that partition
      // will then be the ones that we use to force the new membership and unlock the cluster

      if( primary_partition == false ){
        InfoLog.Println( "No primary partition found! Attempting to choose and force a new one ... " )

        sort.Sort( MembersByOnlineNodes(last_view) )

        // now the last element in the array is the one to use as it's coordinating with the most nodes 
        view_len := len(last_view)-1
        seed_node = &last_view[view_len]

        // *BUT*, if there's no clear winner based on sub-partition size, then we should pick the sub-partition (which
        // can be 1 node) that has executed the most GTIDs
        if( last_view[view_len].Online_participants == last_view[view_len-1].Online_participants ){
          bestmemberpos := view_len
          bestmembertrxcnt := 0
          curtrxcnt := 0
          bestmembertrxcnt, err = last_view[view_len].TransactionCount()

          // let's loop backwards through the array as it's sorted by online participants / partition size now
          // skipping the last one as we already have the info for it
          for i := view_len-1; i >= 0; i-- {
            if( last_view[i].Online_participants == last_view[bestmemberpos].Online_participants ){
              curtrxcnt, err = last_view[i].TransactionCount()
              
              if( curtrxcnt > bestmembertrxcnt ){
                bestmembertrxcnt = curtrxcnt
                bestmemberpos = i
              }
            } else {
              // otherwise we've gone backwards far enough and we have the best option 
              break
            }
          }
        
          seed_node = &last_view[bestmemberpos]
        }
        
        err = seed_node.Connect()
      
        if( err != nil ){
          // let's just loop again 
          continue 
        }

        if( debug ){
          DebugLog.Printf( "Member view sorted by number of online nodes: %+v\n", last_view )
        } 

        // let's build a string of '<host>:<port>' combinations that we want to use for the new membership view
        members, _ := seed_node.GetMembers()

        force_member_string := ""

        for i, member := range *members {
          err = member.Connect()

          if( err == nil && member.Member_state == "ONLINE" ){
            if( i != 0 ){
              force_member_string = force_member_string + ","
            }
 
            force_member_string = force_member_string + member.Mysql_host + ":" + member.Mysql_port
          } else {
            member.Member_state = "SHOOT_ME"
          }
        }

        if( force_member_string != "" ){
          InfoLog.Printf( "Forcing group membership to form new primary partition! Using: '%s'\n", force_member_string )

          err := seed_node.ForceMembers( force_member_string ) 
       
          if( err != nil ){
            InfoLog.Printf( "Error forcing group membership: %v\n", err )
          } else {
            // We successfully unblocked the group, now let's try and politely STONITH the nodes in the losing partition 
            for _, member := range *members {
              if( member.Member_state == "SHOOT_ME" ){
                member.Shutdown()
              }

              if( err != nil ){
                InfoLog.Printf( "Could not shutdown instance: '%s:%s'\n", member.Mysql_host, member.Mysql_port )
              }
            }
          }
        } else {
          InfoLog.Println( "No valid group membership to force!" )
        }
      }
    }
    
    if( err != nil ){
      loop = false
    } else {
      time.Sleep( time.Millisecond * 2000 )
    }
  }

  return err
}


// The remaining functions are used to sort our membership slice
func (a MembersByOnlineNodes) Len() int {
  return len(a)
}

func (a MembersByOnlineNodes) Swap( i, j int ) {
  a[i], a[j] = a[j], a[i]
}

func (a MembersByOnlineNodes) Less( i, j int ) bool {
  return a[i].Online_participants < a[j].Online_participants
}
