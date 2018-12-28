#!/bin/bash

KEYSPACE=cloudcourse
TABLE=g2q1

cat <<- +++
CREATE  KEYSPACE IF NOT EXISTS ${KEYSPACE} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

DROP TABLE ${KEYSPACE}.${TABLE};

CREATE TABLE ${KEYSPACE}.${TABLE} (ORIGIN text, CARRIER text, DEPDELAY double, PRIMARY KEY (ORIGIN, CARRIER));

+++

hdfs dfs -cat /g2q1/* | tr "_" " " | awk -v table=${KEYSPACE}.${TABLE} '
{
	printf("INSERT INTO %s (ORIGIN, CARRIER, DEPDELAY) VALUES ('"'%s'"', '"'%s'"', %f);", table, $1, $2, $3);
	print "";
}' 

cat <<- +++
SELECT * FROM ${KEYSPACE}.${TABLE};
+++
