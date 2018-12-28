#!/bin/bash

KEYSPACE=cloudcourse
TABLE=g2q3

cat <<- +++
CREATE  KEYSPACE IF NOT EXISTS ${KEYSPACE} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

DROP TABLE ${KEYSPACE}.${TABLE};

CREATE TABLE ${KEYSPACE}.${TABLE} (ORIGIN text, DEST text, CARRIER text, ARRDELAY double, PRIMARY KEY (ORIGIN, DEST));

+++

hdfs dfs -cat /g2q3/* | tr "_" " " | awk -v table=${KEYSPACE}.${TABLE} '
{
	printf("INSERT INTO %s (ORIGIN, DEST, CARRIER, ARRDELAY) VALUES ('"'%s'"', '"'%s'"', '"'%s'"', %f);", table, $1, $2, $3, $4);
	print "";
}' 

cat <<- +++
SELECT * FROM ${KEYSPACE}.${TABLE};
+++
