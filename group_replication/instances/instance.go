/*
Copyright 2017 Matthew Lord (mattalord@gmail.com) 

WARNING: This is experimental and for demonstration purposes only!

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package instances

import (
  "database/sql"
  _ "github.com/go-sql-driver/mysql"
  "errors"
  "strings"
  "strconv"
  "log"
  "os"
)

// member variables that start with capital letters are public/exported 
type Instance struct {
  Mysql_host string 
  Mysql_port string
  Mysql_user string
  mysql_pass string

  // The status related vars can serve as an effective cache 
  Group_name string
  Server_uuid string
  Member_state string
  Online_participants uint8
  Has_quorum bool
  Read_only bool
  Applier_queue_size uint16
  db *sql.DB
}

// enable debug logging for all instances
var Debug bool = false

// setup debug logging for all instances
var DebugLog = log.New(os.Stderr,
               "DEBUG: ",
               log.Ldate|log.Ltime|log.Lshortfile)


func New( myh string, myp string, myu string, mys string ) * Instance {
  return &Instance{ Mysql_host: myh, Mysql_port: myp, Mysql_user: myu, mysql_pass: mys }
}

func (me *Instance) Connect() error {
  var err error 

  if( me.db == nil ){
    conn_string := me.Mysql_user + ":" + me.mysql_pass + "@tcp(" + me.Mysql_host + ":" + me.Mysql_port + ")/performance_schema"

    if( Debug ){
      DebugLog.Printf( "Making SQL connection using: %s\n", conn_string )
    }

    me.db, err = sql.Open( "mysql", conn_string )

    if( err != nil ){
      DebugLog.Printf( "Error during sql.Open: %v", err )
    }
  }

  err = me.db.Ping()

  if( err == nil ){
    query_str := "SELECT variable_value FROM global_variables WHERE variable_name='group_replication_group_name'"

    if( Debug ){
      DebugLog.Printf( "Checking group name on '%s:%s'. Query: %s\n", me.Mysql_host, me.Mysql_port, query_str )
    }

    err = me.db.QueryRow( query_str ).Scan( &me.Group_name )

    if( err != nil || me.Group_name == "" ){
      err = errors.New( "Specified MySQL Instance is not a member of any Group Replication cluster!" )
    }

    query_str = "SELECT variable_value, member_state FROM global_variables gv INNER JOIN replication_group_members rgm ON(gv.variable_value=rgm.member_id) WHERE gv.variable_name='server_uuid'"

    if( Debug ){
      DebugLog.Printf( "Checking status of '%s:%s'. Query: %s\n", me.Mysql_host, me.Mysql_port, query_str )
    }

    err = me.db.QueryRow( query_str ).Scan( &me.Server_uuid, &me.Member_state )
  }
  
  return err
}

func (me *Instance) HasQuorum() (bool, error) {
  quorum_query := "SELECT IF( MEMBER_STATE='ONLINE' AND ((SELECT COUNT(*) FROM replication_group_members WHERE MEMBER_STATE != 'ONLINE') >= ((SELECT COUNT(*) FROM replication_group_members)/2) = 0), 'true', 'false' ) FROM replication_group_members JOIN replication_group_member_stats USING(member_id)"

  if( Debug ){
    DebugLog.Printf( "Checking if '%s:%s' has a quorum. Query: %s\n", me.Mysql_host, me.Mysql_port, quorum_query )
  }

  err := me.db.Ping()

  if( err == nil ){
    err = me.db.QueryRow( quorum_query ).Scan( &me.Has_quorum )
  }
 
  return me.Has_quorum, err
}

func (me *Instance) IsReadOnly() (bool, error) {
  ro_query := "SELECT variable_value FROM global_variables WHERE variable_name='super_read_only'"

  if( Debug ){
    DebugLog.Printf( "Checking if '%s:%s' is read only. Query: %s\n", me.Mysql_host, me.Mysql_port, ro_query )
  }

  err := me.db.Ping()

  if( err == nil ){
    err = me.db.QueryRow( ro_query ).Scan( &me.Read_only )
  }

  return me.Read_only, err
}

func (me *Instance) GetMembers() (*[]Instance, error) {
  membership_query := "SELECT member_id, member_host, member_port, member_state FROM replication_group_members"
  member_slice := []Instance{}
  me.Online_participants = 0

  if( Debug ){
    DebugLog.Printf( "Getting group members from '%s:%s'. Query: %s\n", me.Mysql_host, me.Mysql_port, membership_query )
  }

  err := me.db.Ping()

  if( err == nil ){
    rows, err := me.db.Query( membership_query )

    if( err == nil ){
      defer rows.Close()

      for( rows.Next() ){
        member := New( "", "", me.Mysql_user, me.mysql_pass )
        err = rows.Scan( &member.Server_uuid, &member.Mysql_host, &member.Mysql_port, &member.Member_state )
        if( member.Member_state == "ONLINE" ){
          me.Online_participants++ 
        }
        member_slice = append( member_slice, *member )
      }

      if( Debug ){
        DebugLog.Printf( "Group member info found for '%s:%s' -- ONLINE member count: %d, Members: %+v\n", me.Mysql_host, me.Mysql_port, me.Online_participants, member_slice )
      }
    }
  }

  return &member_slice, err 
}

func (me *Instance) Shutdown() error {
  shutdown_query := "SHUTDOWN"

  if( Debug ){
    DebugLog.Printf( "Shutting down node '%s:%s'\n", me.Mysql_host, me.Mysql_port )
  }

  err := me.db.Ping()

  if( err == nil ){
    _, err = me.db.Exec( shutdown_query )
  }

  return err
}

func (me *Instance) TransactionsExecuted() (string, error) {
  // since this is such a fast changing metric, I won't cache the value in the struct
  var gtids string
  gtid_query := "SELECT @@global.GTID_EXECUTED"

  if( Debug ){
    DebugLog.Printf( "Getting the transactions executed on '%s:%s'\n", me.Mysql_host, me.Mysql_port )
  }

  err := me.db.Ping()

  if( err == nil ){
    err = me.db.QueryRow( gtid_query ).Scan( &gtids )
  }

  return gtids, err
}

/* 
 We need to count all of the GTIDs in total
 An example set being:
"39a07a39-4b82-44d2-a3cd-978511564a57:1-37,
49311a3a-e058-46ba-8e7b-857b5db7d33f:1,
550fa9ee-a1f8-4b6d-9bfe-c03c12cd1c72:1-550757:1001496-1749225:2001496-2835762,
de6858e8-0669-4b82-a188-d2906daa6d91:1-119927"
With the total transaction count for that set being: 2252719
*/
func (me *Instance) TransactionCount() (int, error) {  
  gtid_set, err := me.TransactionsExecuted()
  gtid_count := 0 

  if( err == nil ){
    if( Debug ){
      DebugLog.Printf( "Calculating total number of GTIDs from a set of: %s\n", gtid_set )
    }

    next_dash_pos := 0
    next_colon_pos := 0
    next_comma_pos := 0
    colon_pos := strings.IndexRune( gtid_set, ':' )

    for colon_pos != -1 { 
      // lets get rid of everything before the current colon, and the colon itself, as it's UUID info that we don't care about
      gtid_set = gtid_set[colon_pos+1:]
       
      next_dash_pos = strings.IndexRune( gtid_set, '-' )
      next_colon_pos = strings.IndexRune( gtid_set, ':' )
      next_comma_pos = strings.IndexRune( gtid_set, ',' )
       
      firstval := 0
      secondval := 0
      nextval := 0

      if( next_dash_pos < next_colon_pos && next_dash_pos < next_comma_pos ){
        if( next_colon_pos < next_comma_pos ){
          firstval, err = strconv.Atoi( gtid_set[:next_dash_pos] )
          secondval, err = strconv.Atoi( gtid_set[next_dash_pos+1 : next_colon_pos] )

          // the first GTID counts too 
          firstval = firstval-1

          nextval = secondval - firstval
        } else {
          firstval, err = strconv.Atoi( gtid_set[:next_dash_pos] )
          secondval, err = strconv.Atoi( gtid_set[next_dash_pos+1 : next_comma_pos] )

          // the first GTID counts too 
          firstval = firstval-1

          nextval = secondval - firstval
        }
      } else if( next_colon_pos == -1 && next_dash_pos != -1 ){
        firstval, err = strconv.Atoi( gtid_set[:next_dash_pos] )
        secondval, err = strconv.Atoi( gtid_set[next_dash_pos+1:] )

        // the first GTID counts too 
        firstval = firstval-1

        nextval = secondval - firstval
      } else {
        nextval = 1
      }

      if( err != nil ){
        break
      }

      if( Debug ){
        DebugLog.Printf( "The current calculation is: (%d - %d)\n", secondval, firstval )
        DebugLog.Printf( "Current total: %d, adding %d\n", gtid_count, nextval )
      }

      gtid_count = gtid_count + nextval

      colon_pos = strings.IndexRune( gtid_set, ':' )

      if( Debug ){
        DebugLog.Printf( "Remaining unprocessed GTID string: %s\n", gtid_set )
      }
    }
  }             
         
  return gtid_count, err
}   

func (me *Instance) ForceMembers( fms string ) error {
  force_membership_query := "SET GLOBAL group_replication_force_members='" + fms + "'"

  if( Debug ){
    DebugLog.Printf( "Forcing group membership on '%s:%s'. Query: %s\n", me.Mysql_host, me.Mysql_port, force_membership_query )
  }

  err := me.db.Ping()

  if( err == nil ){
    _, err = me.db.Exec( force_membership_query )
  }

  return err
}
