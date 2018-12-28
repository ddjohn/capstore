#!/bin/bash

# Source environment
source env.sh

# Data Cleaning
data/collect.sh process
data/collect.sh hdfs

# Clean up any previuous jobs
hdfs dfs -rm -r /output

# Data Processing
hadoop jar cloudcourse.jar cloudcourse.g1q1.MyMain
hdfs dfs -cat /output/* | sort -n +1 -r | head -10
hdfs dfs -mv /output /G1Q1

hadoop jar cloudcourse.jar cloudcourse.g1q2.MyMain
hdfs dfs -cat /output/* | awk '{print $NF, $0}' | sort -n | head -10
hdfs dfs -mv /output /G1Q2

hadoop jar cloudcourse.jar cloudcourse.g1q3.MyMain
hdfs dfs -cat /output/* | awk '{print $NF, $0}' | sort -n | head -10
hdfs dfs -mv /output /G1Q3

hadoop jar cloudcourse.jar cloudcourse.g2q1.MyMain
hdfs dfs -cat /output/* | tr "_" " " | grep "^CMI" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^BWI" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^MIA" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^LAX" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^IAH" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^SFO" | sort -n -k3 | head -10
hdfs dfs -mv /output /G2Q1

hadoop jar cloudcourse.jar cloudcourse.g2q2.MyMain
hdfs dfs -cat /output/* | tr "_" " " | grep "^CMI" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^BWI" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^MIA" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^LAX" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^IAH" | sort -n -k3 | head -10
hdfs dfs -cat /output/* | tr "_" " " | grep "^SFO" | sort -n -k3 | head -10
hdfs dfs -mv /output /G2Q2

hadoop jar cloudcourse.jar cloudcourse.g2q3.MyMain
hdfs dfs -cat /output/* | grep "^CMI_ORD" | sort -n -k2 | head -10
hdfs dfs -cat /output/* | grep "^IND_CMH" | sort -n -k2 | head -10
hdfs dfs -cat /output/* | grep "^DFW_IAH" | sort -n -k2 | head -10
hdfs dfs -cat /output/* | grep "^LAX_SFO" | sort -n -k2 | head -10
hdfs dfs -cat /output/* | grep "^JFK_LAX" | sort -n -k2 | head -10
hdfs dfs -cat /output/* | grep "^ATL_PHX" | sort -n -k2 | head -10
hdfs dfs -mv /output /G2Q3

hadoop jar cloudcourse.jar cloudcourse.g2q4.MyMain
hdfs dfs -cat /output/* | grep "^CMI_ORD" 
hdfs dfs -cat /output/* | grep "^IND_CMH" 
hdfs dfs -cat /output/* | grep "^DFW_IAH" 
hdfs dfs -cat /output/* | grep "^LAX_SFO" 
hdfs dfs -cat /output/* | grep "^JFK_LAX" 
hdfs dfs -cat /output/* | grep "^ATL_PHX" 
hdfs dfs -mv /output /G2Q4

# G3Q2
(cd data && ./g3q2.sh)

# Load Cassandra
cassandra/g2q1.sh | cqlsh
cassandra/g2q2.sh | cqlsh
cassandra/g2q3.sh | cqlsh
cassandra/g2q4.sh | cqlsh
