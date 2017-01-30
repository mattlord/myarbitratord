# myarbitratord

This deamon will attempt to automatically handle network partitions of various kinds and ensure that the overall
[MySQL Group Replication](https://www.mysql.com/products/enterprise/high_availability.html) service remains alive and healthy.  

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
