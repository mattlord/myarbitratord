/*
Copyright 2017 Matthew Lord (mattalord@gmail.com) 

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package main

import (
  "os"
  "fmt"
  "log"
  "github.com/mattlord/myarbitratord/group_replication/instances"
  "time"
// "flag"
  "sort"
)

type MembersByOnlineNodes []instances.Instance

func main(){
  if( len(os.Args) < 3 ){
    fmt.Println( "myarbitratord usage: myarbitratord <seed_host> <seed_port>" )
    os.Exit(1);
  }

  seed_host := os.Args[1]
  seed_port := os.Args[2]

  // Currently these credentials should work on any participating MySQL instance 
  mysql_user := "root"
  mysql_pass := "!root19M"

  // check out flag.String( "foo" ) for option handling ... 

  fmt.Println( "Welcome to the MySQL Group Replication Arbitrator!" )

  fmt.Printf( "Starting operations from seed node: '%s:%s'\n", seed_host, seed_port )

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
    // let's check the status of the current seed node
    // if the seed node 
    err := seed_node.Connect()
  
    if( err != nil || seed_node.Member_state != "ONLINE" ){
      // if we couldn't connect to the current seed node or it's no longer part of the group
      // let's try and get a new seed node from the last known membership view 
      for _, member := range last_view {
        member.Connect()
        if( err == nil && member.Member_state == "ONLINE" ){
          seed_node = &member
          fmt.Println( "Updated seed node! New seed node is: ", seed_node.Mysql_host, ":", seed_node.Mysql_port ) 
          break
        }
      }
    }

    members, err := seed_node.GetMembers()

    if( err != nil ){
      log.Fatal( err )
    }

    // save this view in case the seed node is no longer valid next time 
    //last_view = copy( last_view, members )
    last_view = *members

    quorum, err := seed_node.HasQuorum()

    if( err != nil ){
      log.Fatal( err )
    }

    if( quorum ){
      // Let's try and shutdown the nodes NOT in the primary partition if we can reach them from the arbitrator 

      for _, member := range *members {
        if( member.Member_state == "ERROR" || member.Member_state == "UNREACHABLE" ){
          fmt.Println( "Shutting down non-healthy node: ", member.Mysql_host, ":", member.Mysql_port )
          err = member.Shutdown()
       
          if( err != nil ){
            fmt.Println( "Could not shutdown instance: ", member.Mysql_host, ":", member.Mysql_port )
          }
        } 
      }
    } else {
      // handling network partitions and split brain scenarios will be much trickier... I'll need to try and
      // contact each member in the last seen view and try to determine which partition should become the
      // primary one we'll then need to contact 1 node in the primary partition and explicitly set the new
      // membership with 'set global group_replication_force_members="<node_list>"' and finally we'll need
      // to try and connect to the nodes on the losing side of the partition and attempt to shutdown mysqld 

      // does anyone have a quorum? Let's double check before forcing the membership 
      primary_partition := false
      for _, member := range last_view {
        member.Connect()    
        quorum, err := member.HasQuorum()
        if( err == nil && quorum ){
          seed_node = &member
          primary_partition = true
          break
        }
      }

      // If noone in fact has a quorum, then let's see which partition has the most
      // online/participating/communicating members; the participants in that partition
      // will then be the ones that we use to force the new membership and unlock the cluster
      // ToDo: should we consider GTID_EXECUTED sets when choosing a partition???
      if( primary_partition == false ){
        sort.Sort( MembersByOnlineNodes(last_view) )
        // now the last element in the array is the one to use as it's coordinating with the most nodes 
        seed_node = &last_view[len(last_view)-1]

        // let's build a string of '<host>:<port>' combinations that we want to use for the new membership view
        members, _ := seed_node.GetMembers()
        force_member_string := ""
        for i, member := range *members {
          if( member.Member_state == "ONLINE" ){
            if( i != 0 ){
              force_member_string = force_member_string + ","
            }
            force_member_string = force_member_string + member.Mysql_host + ":" + member.Mysql_port
          }
        }

        fmt.Println( "Forcing group membership! using: '", force_member_string, "'" )
        err = seed_node.ForceMembers( force_member_string ) 
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
