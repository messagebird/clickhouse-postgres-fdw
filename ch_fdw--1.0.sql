/* ch_fdw/ch_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION ch_fdw" to load this extension. \quit

CREATE FUNCTION ch_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION ch_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER ch_fdw
  HANDLER ch_fdw_handler
  VALIDATOR ch_fdw_validator;

-- CREATE SERVER "ch-fdw"
--   FOREIGN DATA WRAPPER ch_fdw;
