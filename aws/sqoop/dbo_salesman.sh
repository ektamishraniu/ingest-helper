#!/bin/bash
#
# This file is meant to be run from an EMR cluster.

PASS='{ as400_passwd }'

COLUMNS="ID, Name, Status"
TABLE="dbo.Salesman"
FILTERS="1=1"

hdfs dfs -rm -r /tmp/as400_dbo_salesman

sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true --connect '{ as400_conn }' \
--username tdframeworkdev \
--password $PASS \
--query "SELECT ${COLUMNS} FROM ${TABLE} WHERE ${FILTERS} AND \$CONDITIONS" \
--split-by 'ID' \
--m 1 \
--hive-import \
--hive-overwrite \
--hive-database 'sqoop' \
--hive-table 'as400_dbo_salesman' \
--hive-delims-replacement ' ' \
--null-string '\\N' \
--null-non-string '\\N' \
--target-dir '/tmp/as400_dbo_salesman'
