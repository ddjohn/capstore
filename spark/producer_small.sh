#!/bin/bash -xv

TOPIC=cloudcourse

hdfs dfs -cat /input_small | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ${TOPIC}

