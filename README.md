# myarbitratord

This deamon will attempt to automatically handle network partitions of various kinds and ensure that the overall
[MySQL Group Replication](https://www.mysql.com/products/enterprise/high_availability.html) service remains alive and healthy.  

```
Usage of myarbitratord:
  -debug
    	Execute in debug mode with all debug logging enabled
  -mysql_pass string
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
Reading MySQL credentials from file: /Users/matt/.my.json
Unmarshaled mysql auth file contents: {root xxxxx}
Read mysql auth info from file. user: root, password: xxxxx
Welcome to the MySQL Group Replication Arbitrator!
Starting operations from seed node: 'hanode2:3306'
Making SQL connection using: root:xxxxx@tcp(hanode2:3306)/performance_schema
Checking group name on 'hanode2:3306'. Query: SELECT variable_value FROM global_variables WHERE variable_name='group_replication_group_name'
Checking status of 'hanode2:3306'. Query: SELECT variable_value, member_state FROM global_variables gv INNER JOIN replication_group_members rgm ON(gv.variable_value=rgm.member_id) WHERE gv.variable_name='server_uuid'
Getting group members from 'hanode2:3306'. Query: SELECT member_id, member_host, member_port, member_state FROM replication_group_members
Group member info found for 'hanode2:3306' -- ONLINE member count: 3, Members: [{hanode2 3306 root xxxxx  39a07a39-4b82-44d2-a3cd-978511564a57 ONLINE 0 false false 0 <nil>} {hanode3 3306 root xxxxx  49311a3a-e058-46ba-8e7b-857b5db7d33f ONLINE 0 false false 0 <nil>} {hanode4 3306 root xxxxx  de6858e8-0669-4b82-a188-d2906daa6d91 ONLINE 0 false false 0 <nil>}]
Checking if 'hanode2:3306' has a quorum. Query: SELECT IF( MEMBER_STATE='ONLINE' AND ((SELECT COUNT(*) FROM replication_group_members WHERE MEMBER_STATE != 'ONLINE') >= ((SELECT COUNT(*) FROM replication_group_members)/2) = 0), 'true', 'false' ) FROM replication_group_members JOIN replication_group_member_stats USING(member_id)
Checking group name on 'hanode2:3306'. Query: SELECT variable_value FROM global_variables WHERE variable_name='group_replication_group_name'
Checking status of 'hanode2:3306'. Query: SELECT variable_value, member_state FROM global_variables gv INNER JOIN replication_group_members rgm ON(gv.variable_value=rgm.member_id) WHERE gv.variable_name='server_uuid'
Getting group members from 'hanode2:3306'. Query: SELECT member_id, member_host, member_port, member_state FROM replication_group_members
Group member info found for 'hanode2:3306' -- ONLINE member count: 3, Members: [{hanode2 3306 root xxxxx  39a07a39-4b82-44d2-a3cd-978511564a57 ONLINE 0 false false 0 <nil>} {hanode3 3306 root xxxxx  49311a3a-e058-46ba-8e7b-857b5db7d33f ONLINE 0 false false 0 <nil>} {hanode4 3306 root xxxxx  de6858e8-0669-4b82-a188-d2906daa6d91 ONLINE 0 false false 0 <nil>}]
Checking if 'hanode2:3306' has a quorum. Query: SELECT IF( MEMBER_STATE='ONLINE' AND ((SELECT COUNT(*) FROM replication_group_members WHERE MEMBER_STATE != 'ONLINE') >= ((SELECT COUNT(*) FROM replication_group_members)/2) = 0), 'true', 'false' ) FROM replication_group_members JOIN replication_group_member_stats USING(member_id)
...
```

