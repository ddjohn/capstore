#!/bin/bash -xv

TOPIC=cloudcourse

hdfs dfs -cat /input | head -10

hdfs dfs -cat /input | pv | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${TOPIC}

