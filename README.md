# Postgres FDW for Clickhouse

This project aims to provide federated access to [Clickhouse](https://clickhouse.tech/) cluster from [Postgres](https://www.postgresql.org/), with
the ability to write [Foreign Data Wrapper(FDW)](https://wiki.postgresql.org/wiki/Foreign_data_wrappers) API functions in _Go_.

_Here be dragons!_

The thing about this FDW is that, it is written in Go leveraging [CGo](https://golang.org/cmd/cgo/).

See [design.md](design.md) to make your own mind :) 

## Status
This project should be considered experimental.

There is no active developement and usage within MessageBird but feel free to fork/extend this project.

We hope that the code can serve as a reference for a working FDW and to some extent a sample usage of Golang/C interop.

The FDW has been tested to work with Postgres 13 and Clickhouse 20.3.19.4 on Ubuntu 20.04.

But should work on any unix/linux OS that has PG 13 library headers and Clickhouse.

The FDW doesn't rely on any specific features of Clickhouse(CH) so it should work with any version of CH available.

## Getting Started

Clone this repository.

Spin up a container from the Dockerfile present in the project.

*    ```bash
    docker build --pull -f Dockerfile -t ch_fdw:$SOME_VERSION .
    ```
*   ```bash
    docker run -it --rm $IMAGE_ID
    ```

*   ```bash
    ./setup_test_env.sh # see the source of script to manually build the extension
    ```

### Prerequisites for development

Postgres development library headers(PG version 13), go 1.15 and Clickhouse.

See Dockefile for verbose installation instructions.

For local development, install PG on mac with
```
    brew install postgres
```
Build and testing env are most easily available on ubuntu (either docker images or native).

To locate the postgres library sources on either mac/linux, you can do:
```bash
pg_config --includedir-server
# or rather to see all the options, just do
pg_config
```

#### Installing

This FDW uses the [Postgres extension build infrastructure](https://www.postgresql.org/docs/13/extend-pgxs.html)

Makefile should take care of builing and installing the FDW.

```bash
make clean
# There is an extra step of building `c-archive` from the go code.
# `make go` will build the archive library that should be linked with extension c code.
make go
make
make install # or make installcheck
```

#### Sample Usage

After installation of extensions, restart PG server and enter into `psql` shell.

Make sure that corresponding table also exists in CH (provide CH table name in options) and have matching column datatypes and names.

```
shell# su postgres -c psql
psql (11.1 (Ubuntu 11.1-1.pgdg18.04+1))
Type "help" for help.

postgres=# create extension ch_fdw ;
postgres=# create server ch_fdw foreign data wrapper ch_fdw options(host '127.0.0.1', port '9000');
postgres=# create foreign table gotest (name text not null, value text not null, num bigint not null, val numeric, val2 numeric, val3 real, val4 numeric, val5 date, val6 timestamptz) server ch_fdw options (table 'gotest');
postgres=# SELECT * FROM gotest;
INFO:  passed columns [name value num val val2 val3 val4 val5 val6]
INFO:  passed table gotest
 name  | value | num | val  | val2 | val3 | val4 |    val5    |             val6             
-------+-------+-----+------+------+------+------+------------+------------------------------
 hello | world | 110 | 1101 |   11 |  1.1 |    1 | 01-08-2019 | Tue Jan 08 01:11:34 2019 PST
(1 row)

postgres=# -- check that target columns are minimized
postgres=# SELECT name, val, val3 FROM gotest;
INFO:  passed columns [name val val3]
INFO:  passed table gotest
 name  | val  | val3 
-------+------+------
 hello | 1101 |  1.1
(1 row)

postgres=# -- check timestamp conversions
postgres=# SELECT val6 AT TIME ZONE 'CET' FROM gotest ;
INFO:  passed columns [val6]
INFO:  passed table gotest
         timezone         
--------------------------
 Tue Jan 08 10:11:34 2019
(1 row)

postgres=# SELECT val6 AT TIME ZONE 'UTC' FROM gotest ;
INFO:  passed columns [val6]
INFO:  passed table gotest
         timezone         
--------------------------
 Tue Jan 08 09:11:34 2019
(1 row)

postgres=# -- convert CH float to numeric in PG , shows that not all columns necessarily need to be mapped, it's only needs to be correct.
postgres=# create foreign table gotest2 (name text not null, val3 numeric) server ch_fdw options (table 'gotest');
SELECT * FROM gotest2;
INFO:  passed columns [name val3]
INFO:  passed table gotest
 name  | val3 
-------+------
 hello |  1.1
(1 row)

```
## Features

See [ch_fdw.out](expected/ch_fdw.out) for details on features supported.

Supports
* WHERE clauses pushdown
* Aggregate pushdown
