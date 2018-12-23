#!/bin/bash

RESULT=part
LOADER=collect.pig

export HADOOP_ROOT_LOGGER="WARN,console"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

if [ -n "$1" ]
then
for FILE in /cloud/aviation/air_carrier_statistics_ALL/t-100_domestic_market/*.zip
do
	echo "#"
	echo "# Scanning ${FILE} ..."
	echo "#"
	YEAR=$(echo ${FILE} | awk -F/ '{print $NF}' | sed 's/.zip//')
	mkdir -p ${YEAR}
	unzip -o ${FILE} -d ${YEAR}
	pig -x local -4 nolog.conf -f ${LOADER} -param FILE=${YEAR}/*_All.csv | tail +4 > ${YEAR}/${RESULT}
	rm ${YEAR}/*_All.csv
	echo ""
done
else
	cat */${RESULT} > input
	hdfs dfs -rm /input
	hdfs dfs -copyFromLocal input /input
	hdfs dfs -ls /input
fi

