net start mysql;
CREATE DATABASE con CHARACTER SET utf8;
localhost 3306 root 123456

net start redis;
localhost 6379

http://kafka.apache.org/downloads
cmd1 by admin:
    cd /d D:\sdk\kafka2.8\bin\windows
    zookeeper-server-start ../../config/zookeeper.properties
cmd2 by admin:
    cd /d D:\sdk\kafka2.8\bin\windows
    kafka-server-start ../../config/server.properties
kafka.bootstrap.servers=localhost:9092
kafka.zookeeper.connect=localhost:2181


http://localhost:8082
