# ch_fdw/Makefile
# Makefile follows the PG Extension build Infra https://www.postgresql.org/docs/11/extend-pgxs.html
# All the variables like `MODULE_big` can be looked up for details in the above linked docs.
# In short, this make file looks for `ch_fdw.c` and links it with available `ch_fdw.a`
# and using info from `control` file creates and install the PG extension

# a shared library to build from multiple source files (list object files in OBJS)
MODULE_big = ch_fdw
OBJS = ch_fdw.o ch_helpers.o

# this lib gets added to `MODULE_big` link line
# `ch_fdw.a` contains the definitions for extern functions exported from GO with declarations in generated `ch_fdw.h`
# and is passed to the linker in the last step when creating the shared lib `ch_fdw.so`
SHLIB_LINK = ch_fdw.a

# extension name, `.control` file is required with the given name 
EXTENSION = ch_fdw
# random files to install into prefix/share/$MODULEDIR
DATA = ch_fdw--1.0.sql

# list of regression test cases (without suffix), see below
REGRESS = ch_fdw

# extra files to remove in make clean
EXTRA_CLEAN = ch_fdw.a ch_fdw.h

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# custom rule to build archive lib to be used by c code
go: ch_fdw.go chserver.go query.go deparse.go
	go build -buildmode=c-archive -o ch_fdw.a ch_fdw.go chserver.go query.go deparse.go