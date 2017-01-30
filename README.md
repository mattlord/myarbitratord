# myarbitratord

This deamon will attempt to automatically handle network partitions of various kinds and ensure that the overall
[MySQL Group Replication](https://www.mysql.com/products/enterprise/high_availability.html) service remains alive and healthy.  

Usage: myarbitratord <seed_host> <seed_port> [true] (enable debug logging)
