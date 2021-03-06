/*
  Copyright 2017 Matthew Lord (mattalord@gmail.com)

  WARNING: This is experimental and for demonstration purposes only!

  Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

   3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package group

import (
	"database/sql"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	// Anonymous import is required: http://go-database-sql.org/importing.html
	_ "github.com/go-sql-driver/mysql"
)

// Node represents a mysqld process participating in a Group Replication cluster
type Node struct {
	MySQLHost string `json:"MySQL Host,omitempty"`
	MySQLPort string `json:"MySQL Port,omitempty"`
	MySQLUser string `json:"MySQL User"`
	mysqlPass string

	// The status related vars can serve as an effective cache
	GroupName          string `json:"Group Name,omitempty"`
	ServerUuid         string `json:"Server UUID,omitempty"`
	MemberState        string `json:"Member State,omitempty"`
	OnlineParticipants uint8  `json:"Online Members,omitempty"`
	Quorum             bool   `json:"Has Quorum,omitempty"`
	ReadOnly           bool   `json:"Read Only,omitempty"`
	db                 *sql.DB
}

// enable debug logging for all nodes
var Debug bool = false

// setup debug logging for all nodes
var DebugLog = log.New(os.Stderr,
	"DEBUG: ",
	log.Ldate|log.Ltime|log.Lshortfile)

// let's maintain a simple global pool of database objects for all Nodes
var dbcp map[string]*sql.DB = make(map[string]*sql.DB)

// it can be accessed by multiple threads, so let's protect access to it
var DBCPMutex sync.Mutex

// GR_NAME_QUERY is a static query to get the group name (uuid)
const GR_NAME_QUERY string = "SELECT variable_value FROM global_variables WHERE variable_name='group_replication_group_name'"

// GR_STATUS_QUERY is a static query to get the group status
const GR_STATUS_QUERY string = "SELECT variable_value, member_state FROM global_variables gv INNER JOIN replication_group_members rgm ON(gv.variable_value=rgm.member_id) WHERE gv.variable_name='server_uuid'"

// GR_QUORUM_QUERY is a static query to see if there is a primary partition with a quorum
const GR_QUORUM_QUERY string = "SELECT IF( MEMBER_STATE='ONLINE' AND ((SELECT COUNT(*) FROM replication_group_members WHERE MEMBER_STATE != 'ONLINE') >= ((SELECT COUNT(*) FROM replication_group_members)/2) = 0), 'true', 'false' ) FROM replication_group_members JOIN replication_group_member_stats USING(member_id)"

// GR_RO_QUERY is a static query to see if the node is READ ONLY
const GR_RO_QUERY string = "SELECT variable_value FROM global_variables WHERE variable_name='super_read_only'"

// GR_GTID_QUERY is a static query to see if the node's GTID exected set
const GR_GTID_QUERY string = "SELECT @@global.GTID_EXECUTED"

// GR_MEMBERS_QUERY is a static query to see the current group's members
const GR_MEMBERS_QUERY string = "SELECT member_id, member_host, member_port, member_state FROM replication_group_members"

// GR_GTID_SUBSET_QUERY is a static query to see what GTIDs are in the applier queue on a node
const GR_GTID_SUBSET_QUERY string = "SELECT GTID_SUBTRACT( (SELECT Received_transaction_set FROM performance_schema.replication_connection_status WHERE Channel_name = 'group_replication_applier' ), (SELECT @@global.GTID_EXECUTED) )"

// GR_GCSADDR_QUERY is a static query to get the GCS address for the node
const GR_GCSADDR_QUERY string = "SELECT variable_value FROM global_variables WHERE variable_name='group_replication_local_address'"

func New(myh string, myp string, myu string, mys string) *Node {
	return &Node{MySQLHost: myh, MySQLPort: myp, MySQLUser: myu, mysqlPass: mys}
}

func (me *Node) Connect() error {
	var err error

	if me.MySQLHost == "" || me.MySQLPort == "" {
		err = errors.New("No MySQL endpoint specified!")
	} else {
		if me.db == nil {
			connString := me.MySQLUser + ":" + me.mysqlPass + "@tcp(" + me.MySQLHost + ":" + me.MySQLPort + ")/performance_schema"

			DBCPMutex.Lock()

			if dbcp[connString] == nil {
				if Debug {
					DebugLog.Printf("Making SQL connection and adding it to the pool using: %s\n", connString)
				}

				dbcp[connString], err = sql.Open("mysql", connString)
			}

			if err != nil {
				DebugLog.Printf("Error during sql.Open: %v", err)
			} else {
				me.db = dbcp[connString]
			}

			DBCPMutex.Unlock()
		}

		err = me.db.Ping()

		if err == nil {
			if Debug {
				DebugLog.Printf("Checking group name on '%s:%s'. Query: %s\n", me.MySQLHost, me.MySQLPort, GR_NAME_QUERY)
			}

			err = me.db.QueryRow(GR_NAME_QUERY).Scan(&me.GroupName)

			if err != nil {
				// let's just return the error
			} else if me.GroupName == "" {
				err = errors.New("Specified MySQL Node is not a member of any Group Replication cluster!")
			} else {
				if Debug {
					DebugLog.Printf("Checking status of '%s:%s'. Query: %s\n", me.MySQLHost, me.MySQLPort, GR_STATUS_QUERY)
				}

				err = me.db.QueryRow(GR_STATUS_QUERY).Scan(&me.ServerUuid, &me.MemberState)
			}
		}
	}

	return err
}

func (me *Node) HasQuorum() (bool, error) {
	if Debug {
		DebugLog.Printf("Checking if '%s:%s' has a quorum. Query: %s\n", me.MySQLHost, me.MySQLPort, GR_QUORUM_QUERY)
	}

	err := me.db.Ping()

	if err == nil {
		err = me.db.QueryRow(GR_QUORUM_QUERY).Scan(&me.Quorum)
	}

	return me.Quorum, err
}

func (me *Node) MemberStatus() (string, error) {
	if Debug {
		DebugLog.Printf("Checking member status of '%s:%s'. Query: %s\n", me.MySQLHost, me.MySQLPort, GR_STATUS_QUERY)
	}

	err := me.db.Ping()

	if err == nil {
		err = me.db.QueryRow(GR_STATUS_QUERY).Scan(&me.MemberState)
	}

	return me.MemberState, err
}

func (me *Node) IsReadOnly() (bool, error) {
	if Debug {
		DebugLog.Printf("Checking if '%s:%s' is read only. Query: %s\n", me.MySQLHost, me.MySQLPort, GR_RO_QUERY)
	}

	err := me.db.Ping()

	if err == nil {
		tmpval := "" // will be set to "ON" or "OFF"
		err = me.db.QueryRow(GR_RO_QUERY).Scan(&tmpval)

		if tmpval == "ON" {
			me.ReadOnly = true
		} else {
			me.ReadOnly = false
		}
	}

	return me.ReadOnly, err
}

func (me *Node) GetMembers() ([]Node, error) {
	memberSlice := make([]Node, 0, 3)
	me.OnlineParticipants = 0

	if Debug {
		DebugLog.Printf("Getting group members from '%s:%s'. Query: %s\n", me.MySQLHost, me.MySQLPort, GR_MEMBERS_QUERY)
	}

	err := me.db.Ping()

	if err == nil {
		rows, err := me.db.Query(GR_MEMBERS_QUERY)

		if err == nil {
			defer rows.Close()

			for rows.Next() {
				member := New("", "", me.MySQLUser, me.mysqlPass)
				err = rows.Scan(&member.ServerUuid, &member.MySQLHost, &member.MySQLPort, &member.MemberState)
				if err == nil {
					if member.MemberState == "ONLINE" {
						me.OnlineParticipants++
					}
					memberSlice = append(memberSlice, *member)
				}
			}

			rows.Close()

			if Debug {
				DebugLog.Printf("Group member info found for '%s:%s' -- ONLINE member count: %d, Members: %+v\n", me.MySQLHost, me.MySQLPort, me.OnlineParticipants, memberSlice)
			}
		}
	}

	return memberSlice, err
}

func (me *Node) Shutdown() error {
	ShutdownQuery := "SHUTDOWN"

	if Debug {
		DebugLog.Printf("Shutting down node '%s:%s'\n", me.MySQLHost, me.MySQLPort)
	}

	err := me.db.Ping()

	if err == nil {
		_, err = me.db.Exec(ShutdownQuery)
	}

	return err
}

func (me *Node) TransactionsExecuted() (string, error) {
	// since this is such a fast changing metric, I won't cache the value in the struct
	var gtids string

	if Debug {
		DebugLog.Printf("Getting the transactions executed on '%s:%s'\n", me.MySQLHost, me.MySQLPort)
	}

	err := me.db.Ping()

	if err == nil {
		err = me.db.QueryRow(GR_GTID_QUERY).Scan(&gtids)
	}

	return gtids, err
}

func (me *Node) TransactionsExecutedCount() (uint64, error) {
	var err error
	var GTIDSet string
	var cnt uint64

	GTIDSet, err = me.TransactionsExecuted()

	if err != nil {
		cnt, err = TransactionCount(GTIDSet)
	}

	return cnt, err
}

func (me *Node) ApplierQueueLength() (uint64, error) {
	// since this is such a fast changing metric, I won't cache the value in the struct
	var qlen uint64
	var GTIDSubset string

	if Debug {
		DebugLog.Printf("Getting the applier queue length on '%s:%s'\n", me.MySQLHost, me.MySQLPort)
	}

	err := me.db.Ping()

	if err == nil {
		err = me.db.QueryRow(GR_GTID_SUBSET_QUERY).Scan(&GTIDSubset)
	}

	qlen, err = TransactionCount(GTIDSubset)

	return qlen, err
}

/*
 This is a global function to count a total of all the GTIDs in a set
 An example set being:
"39a07a39-4b82-44d2-a3cd-978511564a57:1-37,
49311a3a-e058-46ba-8e7b-857b5db7d33f:1,
550fa9ee-a1f8-4b6d-9bfe-c03c12cd1c72:1-550757:1001496-1749225:2001496-2835762,
de6858e8-0669-4b82-a188-d2906daa6d91:1-119927"
With the total transaction count for that set being: 2252719
*/
func TransactionCount(GTIDSet string) (uint64, error) {
	var err error
	var GTIDCount uint64
	nextDashPos := 0
	nextcolonPos := 0
	nextCommaPos := 0
	colonPos := strings.IndexRune(GTIDSet, ':')
	var firstval uint64
	var secondval uint64
	var nextval uint64

	if Debug {
		DebugLog.Printf("Calculating total number of GTIDs from a set of: %s\n", GTIDSet)
	}

	for colonPos != -1 {
		// lets get rid of everything before the current colon, and the colon itself, as it's UUID info that we don't care about
		GTIDSet = GTIDSet[colonPos+1:]

		nextDashPos = strings.IndexRune(GTIDSet, '-')
		nextcolonPos = strings.IndexRune(GTIDSet, ':')
		nextCommaPos = strings.IndexRune(GTIDSet, ',')

		firstval = 0
		secondval = 0
		nextval = 0

		if nextDashPos < nextcolonPos && nextDashPos < nextCommaPos {
			if nextcolonPos < nextCommaPos {
				firstval, err = strconv.ParseUint(GTIDSet[:nextDashPos], 10, 64)
				secondval, err = strconv.ParseUint(GTIDSet[nextDashPos+1:nextcolonPos], 10, 64)

				// the first GTID counts too
				firstval = firstval - 1

				nextval = secondval - firstval
			} else {
				firstval, err = strconv.ParseUint(GTIDSet[:nextDashPos], 10, 64)
				secondval, err = strconv.ParseUint(GTIDSet[nextDashPos+1:nextCommaPos], 10, 64)

				// the first GTID counts too
				firstval = firstval - 1

				nextval = secondval - firstval
			}
		} else if nextcolonPos == -1 && nextDashPos != -1 {
			firstval, err = strconv.ParseUint(GTIDSet[:nextDashPos], 10, 64)
			secondval, err = strconv.ParseUint(GTIDSet[nextDashPos+1:], 10, 64)

			// the first GTID counts too
			firstval = firstval - 1

			nextval = secondval - firstval
		} else {
			nextval = 1
		}

		if err != nil {
			break
		}

		if Debug {
			DebugLog.Printf("The current calculation is: (%d - %d)\n", secondval, firstval)
			DebugLog.Printf("Current total: %d, adding %d\n", GTIDCount, nextval)
		}

		GTIDCount = GTIDCount + nextval

		colonPos = strings.IndexRune(GTIDSet, ':')

		if Debug {
			DebugLog.Printf("Remaining unprocessed GTID string: %s\n", GTIDSet)
		}
	}

	return GTIDCount, err
}

func (me *Node) GetGCSAddress() (string, error) {
	var GCSAddr string

	if Debug {
		DebugLog.Printf("Getting GCS endpoint for '%s:%s'. Query: %s\n", me.MySQLHost, me.MySQLPort, GR_GCSADDR_QUERY)
	}

	err := me.db.Ping()

	if err == nil {
		err = me.db.QueryRow(GR_GCSADDR_QUERY).Scan(&GCSAddr)
	}

	return GCSAddr, err
}

func (me *Node) ForceMembers(fms string) error {
	forceMembershipQuery := "SET GLOBAL group_replication_force_members='" + fms + "'"

	if Debug {
		DebugLog.Printf("Forcing group membership on '%s:%s'. Query: %s\n", me.MySQLHost, me.MySQLPort, forceMembershipQuery)
	}

	err := me.db.Ping()

	if err == nil {
		_, err = me.db.Exec(forceMembershipQuery)
	}

	// now that we've forced the membership, let's reset the global variable (otherwise it will cause complications later)
	if err == nil {
		_, err = me.db.Exec("SET GLOBAL group_replication_force_members=''")
	}

	return err
}

func (me *Node) SetReadOnly(ro bool) error {
	ROQuery := "SET GLOBAL super_read_only="

	if ro {
		ROQuery = ROQuery + "ON"
	} else {
		ROQuery = ROQuery + "OFF"
	}

	if Debug {
		DebugLog.Printf("Setting read_only mode to %t on '%s:%s'\n", ro, me.MySQLHost, me.MySQLPort)
	}

	err := me.db.Ping()

	if err == nil {
		_, err = me.db.Exec(ROQuery)
		me.ReadOnly = ro
	}

	return err
}

func (me *Node) SetOfflineMode(om bool) error {
	OMQuery := "SET GLOBAL offline_mode="

	if om {
		OMQuery = OMQuery + "ON"
	} else {
		OMQuery = OMQuery + "OFF"
	}

	if Debug {
		DebugLog.Printf("Setting offline mode to %t on '%s:%s'\n", om, me.MySQLHost, me.MySQLPort)
	}

	err := me.db.Ping()

	if err == nil {
		_, err = me.db.Exec(OMQuery)
	}

	return err
}

func (me *Node) Cleanup() error {
	var err error = nil

	if Debug {
		DebugLog.Printf("Cleaning up Node object for '%s:%s'\n", me.MySQLHost, me.MySQLPort)
	}

	// We don't want to close this anymore as it's a pointer to a connection in our pool now
	/*
	  if( me.db != nil ){
	    err = me.db.Close()
	  }
	*/

	return err
}

func (me *Node) Reset() {
	_ = me.Cleanup()

	if Debug {
		DebugLog.Printf("Resetting Node object for '%s:%s'\n", me.MySQLHost, me.MySQLPort)
	}

	me.MySQLHost = ""
	me.MySQLPort = ""
	me.MySQLUser = ""
	me.mysqlPass = ""
	me.GroupName = ""
	me.ServerUuid = ""
	me.MemberState = ""
	me.OnlineParticipants = 0
	me.Quorum = false
	me.ReadOnly = false
	me.db = nil
}
