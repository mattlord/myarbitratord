# myarbitratord

**WARNING:** **_This is *experimental* and for demonstration purposes only!_**

This deamon will attempt to automatically handle network partitions of various kinds and ensure that the overall
[MySQL Group Replication](https://www.mysql.com/products/enterprise/high_availability.html) service remains alive and healthy.  

```
Usage of myarbitratord:
  -debug
    	Execute in debug mode with all debug logging enabled
  -mysql_auth_file string
    	The JSON encoded file containining user and password entities for the mysql account to be used when connecting to any node in the cluster
  -mysql_password string
    	The mysql user account password to be used when connecting to any node in the cluster
  -mysql_user string
    	The mysql user account to be used when connecting to any node in the cluster (default "root")
  -seed_host string
    	IP/Hostname of the seed node used to start monitoring the Group Replication cluster
  -seed_port string
    	Port of the seed node used to start monitoring the Group Replication cluster (default "3306")
```

Specifying the MySQL credentials on the command-line is insecure as the password is visible in the processlist output and elsewhere. The recommended way to specify the MySQL credentials is using a JSON file which can then be protected at the filesystem level. The format of that JSON file should be:
```json
{
  "user": "myser",
  "password": "mypass"
}
```


Here's an example:
```
gonzo:myarbitratord matt$ $GOBIN/myarbitratord -seed_host="hanode2" -mysql_auth_file="/Users/matt/.my.json" -debug 
DEBUG: 2017/02/01 12:04:15 myarbitratord.go:73: Reading MySQL credentials from file: /Users/matt/.my.json
DEBUG: 2017/02/01 12:04:15 myarbitratord.go:86: Unmarshaled mysql auth file contents: {User:root Password:xxxxx}
DEBUG: 2017/02/01 12:04:15 myarbitratord.go:98: Read mysql auth info from file. user: root, password: xxxxx
INFO: 2017/02/01 12:04:15 myarbitratord.go:102: Welcome to the MySQL Group Replication Arbitrator!
INFO: 2017/02/01 12:04:15 myarbitratord.go:104: Starting operations from seed node: 'hanode2:3306'
DEBUG: 2017/02/01 12:04:15 instance.go:63: Making SQL connection using: root:xxxxx@tcp(hanode2:3306)/performance_schema
DEBUG: 2017/02/01 12:04:15 instance.go:79: Checking group name on 'hanode2:3306'. Query: SELECT variable_value FROM global_variables WHERE variable_name='group_replication_group_name'
DEBUG: 2017/02/01 12:04:15 instance.go:91: Checking status of 'hanode2:3306'. Query: SELECT variable_value, member_state FROM global_variables gv INNER JOIN replication_group_members rgm ON(gv.variable_value=rgm.member_id) WHERE gv.variable_name='server_uuid'
DEBUG: 2017/02/01 12:04:15 instance.go:138: Getting group members from 'hanode2:3306'. Query: SELECT member_id, member_host, member_port, member_state FROM replication_group_members
DEBUG: 2017/02/01 12:04:15 instance.go:159: Group member info found for 'hanode2:3306' -- ONLINE member count: 3, Members: [{Mysql_host:hanode2 Mysql_port:3306 Mysql_user:root mysql_pass:xxxxx Group_name: Server_uuid:39a07a39-4b82-44d2-a3cd-978511564a57 Member_state:ONLINE Online_participants:0 Has_quorum:false Read_only:false Applier_queue_size:0 db:<nil>} {Mysql_host:hanode3 Mysql_port:3306 Mysql_user:root mysql_pass:xxxxx Group_name: Server_uuid:49311a3a-e058-46ba-8e7b-857b5db7d33f Member_state:ONLINE Online_participants:0 Has_quorum:false Read_only:false Applier_queue_size:0 db:<nil>} {Mysql_host:hanode4 Mysql_port:3306 Mysql_user:root mysql_pass:xxxxx Group_name: Server_uuid:de6858e8-0669-4b82-a188-d2906daa6d91 Member_state:ONLINE Online_participants:0 Has_quorum:false Read_only:false Applier_queue_size:0 db:<nil>}]
DEBUG: 2017/02/01 12:04:15 instance.go:104: Checking if 'hanode2:3306' has a quorum. Query: SELECT IF( MEMBER_STATE='ONLINE' AND ((SELECT COUNT(*) FROM replication_group_members WHERE MEMBER_STATE != 'ONLINE') >= ((SELECT COUNT(*) FROM replication_group_members)/2) = 0), 'true', 'false' ) FROM replication_group_members JOIN replication_group_member_stats USING(member_id)
DEBUG: 2017/02/01 12:04:15 myarbitratord.go:162: Seed node details: &{Mysql_host:hanode2 Mysql_port:3306 Mysql_user:root mysql_pass:xxxxx Group_name:550fa9ee-a1f8-4b6d-9bfe-c03c12cd1c72 Server_uuid:39a07a39-4b82-44d2-a3cd-978511564a57 Member_state:ONLINE Online_participants:3 Has_quorum:true Read_only:false Applier_queue_size:0 db:0xc4200b8000}
DEBUG: 2017/02/01 12:04:17 instance.go:79: Checking group name on 'hanode2:3306'. Query: SELECT variable_value FROM global_variables WHERE variable_name='group_replication_group_name'
DEBUG: 2017/02/01 12:04:17 instance.go:91: Checking status of 'hanode2:3306'. Query: SELECT variable_value, member_state FROM global_variables gv INNER JOIN replication_group_members rgm ON(gv.variable_value=rgm.member_id) WHERE gv.variable_name='server_uuid'
DEBUG: 2017/02/01 12:04:17 instance.go:138: Getting group members from 'hanode2:3306'. Query: SELECT member_id, member_host, member_port, member_state FROM replication_group_members
DEBUG: 2017/02/01 12:04:17 instance.go:159: Group member info found for 'hanode2:3306' -- ONLINE member count: 3, Members: [{Mysql_host:hanode2 Mysql_port:3306 Mysql_user:root mysql_pass:xxxxx Group_name: Server_uuid:39a07a39-4b82-44d2-a3cd-978511564a57 Member_state:ONLINE Online_participants:0 Has_quorum:false Read_only:false Applier_queue_size:0 db:<nil>} {Mysql_host:hanode3 Mysql_port:3306 Mysql_user:root mysql_pass:xxxxx Group_name: Server_uuid:49311a3a-e058-46ba-8e7b-857b5db7d33f Member_state:ONLINE Online_participants:0 Has_quorum:false Read_only:false Applier_queue_size:0 db:<nil>} {Mysql_host:hanode4 Mysql_port:3306 Mysql_user:root mysql_pass:xxxxx Group_name: Server_uuid:de6858e8-0669-4b82-a188-d2906daa6d91 Member_state:ONLINE Online_participants:0 Has_quorum:false Read_only:false Applier_queue_size:0 db:<nil>}]
DEBUG: 2017/02/01 12:04:17 instance.go:104: Checking if 'hanode2:3306' has a quorum. Query: SELECT IF( MEMBER_STATE='ONLINE' AND ((SELECT COUNT(*) FROM replication_group_members WHERE MEMBER_STATE != 'ONLINE') >= ((SELECT COUNT(*) FROM replication_group_members)/2) = 0), 'true', 'false' ) FROM replication_group_members JOIN replication_group_member_stats USING(member_id)
DEBUG: 2017/02/01 12:04:17 myarbitratord.go:162: Seed node details: &{Mysql_host:hanode2 Mysql_port:3306 Mysql_user:root mysql_pass:xxxxx Group_name:550fa9ee-a1f8-4b6d-9bfe-c03c12cd1c72 Server_uuid:39a07a39-4b82-44d2-a3cd-978511564a57 Member_state:ONLINE Online_participants:3 Has_quorum:true Read_only:false Applier_queue_size:0 db:0xc4200b8000}
...
```

