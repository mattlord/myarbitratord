# myarbitratord

**WARNING:** **_This is *experimental* and for demonstration purposes only!_**

This deamon will attempt to automatically handle network partitions of various kinds and ensure that the overall
[MySQL Group Replication](https://www.mysql.com/products/enterprise/high_availability.html) service remains alive and healthy.  

```
Usage of myarbitratord:
  -debug
    	Execute in debug mode with all debug logging enabled
  -http_port string
    	The HTTP port used for the RESTful API (default "8099")
  -mysql_auth_file string
    	The JSON encoded file containining user and password entities for the mysql account to be used when connecting to any node in the cluster
  -mysql_password string
    	The mysql user account password to be used when connecting to any node in the cluster
  -mysql_user string
    	The mysql user account to be used when connecting to any node in the cluster (default "root")
  -seed_host string
    	IP/Hostname of the seed node used to start monitoring the Group Replication cluster (Required Parameter!)
  -seed_port string
    	Port of the seed node used to start monitoring the Group Replication cluster (default "3306")
```


## Use Case:
You're a DBA that is tasked with monitoring a Group Replication cluster and ensuring that the distributed MySQL service remains available and healthy from the application's perspective. 


## How It Works:
The deamon performs two functions, both done in distinct threads:
#### A. The RESTful API thread simply provides runtime information on the monitored Group Replication cluster and the myarbitratord operations. See [the API docs](#available-restful-api-calls-with-example-output).

#### B. The main thread connects to a Group Replication cluster via the seed node information specified on the command-line via the -seed_host and -seed_port flags. The thread then loops and performs the following actions:
1. If we see that the previous seed node is no longer reachable or valid, then we'll attempt to get a new seed node from the last known membership view. We don't give up attempting to find a seed node from the last known list of cluster participants.

2. If we see that any nodes previously in the group aren't any more because they were isolated or encountered an error, then we try and shut them down. This helps to prevent dirty reads and lost writes. 

3. If we see that there was a network partition that caused a loss of quorum--which means that the cluster is blocked and cannot proceed without manual intervention--then we will attempt to pick a new primary partition, force the membership of this new group to allow the cluster to proceed, and then shutdown the instances left out of the primary partition.  When choosing the new primary partition, we take the two following factors into account:
  1. If a partition has more online members, then this will be the new primary partition
  2. If there's no clear winner based on partition size, then we will pick the partition that has the largest GTID set 


## Installation:
1. Install golang: https://golang.org/doc/install

2. Setup the build environment: e.g. `export GOBIN=/Users/matt/go-workspace/bin GOPATH=/Users/matt/go-workspace && mkdir $GOPATH`

3. Get the source: `go get "github.com/mattlord/myarbitratord"`

4. Build it: `cd $GOPATH/src/github.com/mattlord/myarbitratord && go install myarbitratord.go` (compiles myarbitratord and places binary in $GOBIN)

5. Run it: `$GOBIN/myarbitratord -help`


## Security:
Specifying the MySQL credentials on the command-line is insecure as the password is visible in the processlist output and elsewhere. The recommended way to specify the MySQL credentials is using a JSON file which can then be protected at the filesystem level. The format of that JSON file should be:
```json
{
  "user": "myser",
  "password": "mypass"
}
```

**Note:** _The RESTful API currently has no authentication mechanism_


## Example:
```
gonzo:myarbitratord matt$ $GOBIN/myarbitratord -seed_host="hanode3" -mysql_auth_file="/Users/matt/.my.json"
INFO: 2017/02/18 13:22:34 myarbitratord.go:138: Starting HTTP server for RESTful API on port 8099
INFO: 2017/02/18 13:22:34 myarbitratord.go:176: Welcome to the MySQL Group Replication Arbitrator!
INFO: 2017/02/18 13:22:34 myarbitratord.go:178: Starting operations from seed node: 'hanode3:3306'
```

## Available RESTful API Calls With Example Output:
**/**
```
gonzo:~ matt$ curl http://localhost:8099/
Welcome to the MySQL Arbitrator's RESTful API handler!

The available API calls are:
/stats: Provide runtime and operational stats
```

**/stats**
```
gonzo:~ matt$ curl http://localhost:8099/stats
{
    "Started": "Fri, 17 Feb 2017 16:03:28 EST",
    "Uptime": "15h40m50.378786739s",
    "Loops": 6181,
    "Partitions": 2,
    "Current Seed Node": {
        "MySQL Host": "hanode3",
        "MySQL Port": "3306",
        "Group Name": "550fa9ee-a1f8-4b6d-9bfe-c03c12cd1c72",
        "Server UUID": "49311a3a-e058-46ba-8e7b-857b5db7d33f",
        "Member State": "ONLINE",
        "Online Members": 3,
        "Has Quorum": true
    },
    "Last Membership View": [
        {
            "MySQL Host": "hanode2",
            "MySQL Port": "3306",
            "Server UUID": "39a07a39-4b82-44d2-a3cd-978511564a57",
            "Member State": "ONLINE"
        },
        {
            "MySQL Host": "hanode3",
            "MySQL Port": "3306",
            "Server UUID": "49311a3a-e058-46ba-8e7b-857b5db7d33f",
            "Member State": "ONLINE"
        },
        {
            "MySQL Host": "hanode4",
            "MySQL Port": "3306",
            "Server UUID": "de6858e8-0669-4b82-a188-d2906daa6d91",
            "Member State": "ONLINE"
        }
    ]
}
```

**/debug/pprof** (only available if binary is built with the "net/http/pprof" import uncommented) See: http://blog.ralch.com/tutorial/golang-performance-and-memory-analysis/
