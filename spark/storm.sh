#!/bin/bash -xv

nohup /opt/kafka/bin/zookeeper-server-start.sh  /opt/kafka/config/zookeeper.properties&
sleep 2

nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties&
sleep 2

