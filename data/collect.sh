#!/bin/bash

RESULT=part
LOADER=collect.pig

export HADOOP_ROOT_LOGGER="WARN,console"
#export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

# Clean PIG files
rm -f pig_*.log

case "$1" in

	process)
		for FILE in /cloud/aviation/airline_ontime/*/*.zip
		do
			echo "#"
			echo "# Scanning ${FILE} ..."
			echo "#"
			YEAR=$(echo ${FILE} | awk -F/ '{print $NF}' | sed 's/.zip//')
			echo ${YEAR}
			mkdir -p ${YEAR}
			unzip -o ${FILE} -d ${YEAR}
			pig -x local -f ${LOADER} -param FILE=${YEAR}/*.csv | tail +2 > ${YEAR}/${RESULT}
			#gzip ${YEAR}/${RESULT}
			rm ${YEAR}/*.csv
			echo ""
		done
		;;

	hdfs)
		cat */${RESULT} > input
		hdfs dfs -rm -r /input
		hdfs dfs -copyFromLocal input /input
		hdfs dfs -find /
		;;

	*)
		echo "syntax: $0 <process|hdfs>" 1>&2
		exit 1
		;;
esac

