#!/bin/bash -xv

TOPIC=cloudcourse

hdfs dfs -cat /input_big | head -10

hdfs dfs -cat /input_big | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${TOPIC}
