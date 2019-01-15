#!/bin/bash -xv

TOPIC=cloudcourse

/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ${TOPIC} --from-beginning

