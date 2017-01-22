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
)

func main(){
  if( len(os.Args) < 3 ){
    fmt.Println( "myarbitratord usage: myarbitratord <seed_host> <seed_port>" )
    os.Exit(1);
  }

  seed_host := os.Args[1]
  seed_port := os.Args[2]

  fmt.Println( "Welcome to the MySQL Group Replication Arbitrator!" )

  fmt.Printf( "Starting operations from seed node: '%s:%s'\n", seed_host, seed_port )

  seed_node := instance.New( seed_host, seed_port, "root", "!root19M" )
  
  err := seed_node.Connect()

  if( err != nil ){
    log.Fatal(err);
  }

  quorum, err := seed_node.HasQuorum()

  if( err != nil ){
    log.Fatal(err);
  }

  fmt.Printf( "Does the seed node have a write quorum? %v\n", quorum ) 

  if( quorum ){
    // spawn 1+ threads to monitor the group and do arbitration 
  }

  os.Exit(0);
}
