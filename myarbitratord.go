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
 "github.com/mattlord/myarbitratord/group_replication/instance"
  "time"
// "flag"
)

func main(){
  if( len(os.Args) < 3 ){
    fmt.Println( "myarbitratord usage: myarbitratord <seed_host> <seed_port>" )
    os.Exit(1);
  }

  seed_host := os.Args[1]
  seed_port := os.Args[2]
  mysql_user := "root"
  mysql_pass := "!root19M"

  // check out flag.String( "foo" ) for option handling ... 

  fmt.Println( "Welcome to the MySQL Group Replication Arbitrator!" )

  fmt.Printf( "Starting operations from seed node: '%s:%s'\n", seed_host, seed_port )

  seed_node := instance.New( seed_host, seed_port, mysql_user, mysql_pass )
  err := MonitorCluster( seed_node )
  
  if( err != nil ){
    log.Fatal( err )
    os.Exit( 100 )
  } else {
    os.Exit( 0 )
  }
}


func MonitorCluster( seed_node *instance.Instance ) error {
  loop := true
  var err error
  last_view := []instance.Instance{}
  
  for( loop == true ){
    //primary_partition := false

    // let's check the status of the current seed node
    // if the seed node 
    err := seed_node.Connect()
  
    if( err != nil || seed_node.Member_state != "ONLINE" ){
      // if we couldn't connect to the current seed node or it's no longer part of the group
      // let's try and get a new seed node from the last known membership view 
      for _, member := range last_view {
        member.Connect()
        if( err == nil && member.Member_state == "ONLINE"){
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

      if( err != nil ){
        log.Fatal( err );
      }

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
      // handling network partitions and split brain scenarios will be much trickier... I'll need to try and contact each member in
      // the last seen view and try to determine which partition should become the primary one we'll then need
      // to contact 1 node in the primary partition and explicitly set the new membership with
      // 'set global group_replication_force_members="<node_list>"'
      // and finally we'll need to try and connect to the nodes on the losing side of the partition and attempt to shutdown mysqld 
    }
    
    if( err != nil ){
      loop = false
    } else {
      time.Sleep( time.Millisecond * 2000 )
    }
  }

  return err
}
