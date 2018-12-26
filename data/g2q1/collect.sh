#!/bin/bash

RESULT=input
LOADER=collect.pig

export HADOOP_ROOT_LOGGER="WARN"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

if [ -n "$1" ]
then
for FILE in /cloud/aviation/airline_ontime/*2000*/*.zip
do
	echo "#"
	echo "# Scanning ${FILE} ..."
	echo "#"
	YEAR=$(echo ${FILE} | awk -F/ '{print $NF}' | sed 's/.zip//')
	echo ${YEAR}
	mkdir -p ${YEAR}
	unzip -o ${FILE} -d ${YEAR}
	pig -x local -4 nolog.conf -f ${LOADER} -param FILE=${YEAR}/*.csv | tail +4 > ${YEAR}/${RESULT}
	rm ${YEAR}/*.csv
	echo ""
done
else
	cat */${RESULT} > input
	hdfs dfs -rm -r /${RESULT}/
	hdfs dfs -copyFromLocal input /${RESULT}
	hdfs dfs -ls /${RESULT}
fi

