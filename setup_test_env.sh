#!/usr/bin/env bash

set -e

make clean
make go
make
make install

service postgresql restart
service clickhouse-server restart

sleep 3
clickhouse-client --query="drop table if exists gotest;"
clickhouse-client --query="drop table if exists gotest2;"
clickhouse-client --query="drop table if exists gotest3;"
clickhouse-client --query="create table gotest(name String, value String, num Int64, val UInt64, val2 UInt32, val3 Float32, val4 Int8, val5 Date, val6 DateTime('CET')) Engine = Log;"
clickhouse-client --query="insert into gotest values('hello', 'world', 110, 1101, 11, 1.1, 1, '2019-01-08', '2019-01-08T10:11:34');"
clickhouse-client --query="create table gotest2(val Array(UInt8), val2 Array(String)) Engine = Log;"
clickhouse-client --query="insert into gotest2 values([1, 2], ['hello', 'world']);"
clickhouse-client --query="create table gotest3(val UInt8, val2 UInt8) Engine = Log;"
clickhouse-client --query="insert into gotest3 values(1, 2);"
clickhouse-client --query="insert into gotest3 values(2, 2);"
clickhouse-client --query="insert into gotest3 values(3, 2);"
clickhouse-client --query="insert into gotest3 values(4, 2);"
su postgres -c "make installcheck"
su postgres -c "psql << EOF
drop extension IF EXISTS ch_fdw cascade;
create extension IF NOT EXISTS ch_fdw ;
create server IF NOT EXISTS ch_fdw foreign data wrapper ch_fdw options(host '127.0.0.1', port '9000');
create foreign table IF NOT EXISTS gotest (name text not null, value text not null, num bigint not null, val numeric, val2 numeric, val3 real, val4 numeric, val5 date, val6 timestamptz) server ch_fdw options (table 'gotest');
SELECT * FROM gotest;
EOF"