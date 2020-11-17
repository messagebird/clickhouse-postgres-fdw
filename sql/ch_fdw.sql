create extension ch_fdw ;
create server ch_fdw foreign data wrapper ch_fdw options(host '127.0.0.1', port '9000');
create foreign table gotest (name text not null, value text not null, num bigint not null, val numeric, val2 numeric, val3 real, val4 numeric, val5 date, val6 timestamptz) server ch_fdw options (table 'gotest');
SELECT * FROM gotest;
-- check that target columns are minimized
SELECT name, val, val3 FROM gotest;
-- check timestamp conversions
SELECT val6 AT TIME ZONE 'CET' FROM gotest ;
SELECT val6 AT TIME ZONE 'UTC' FROM gotest ;
-- convert CH float to numeric in PG , shows that not all columns necessarily need to be mapped, it's only needs to be correct.
create foreign table gotest2 (name text not null, val3 numeric) server ch_fdw options (table 'gotest');
SELECT * FROM gotest2;
-- checks target columns are properly pushed down or not
SELECT val3 FROM gotest WHERE val2 = 11;
SELECT val3 FROM gotest WHERE name IN ('hello', 'world');
SELECT val3 FROM gotest WHERE upper(name) = 'HELLO';
SELECT val3 FROM gotest WHERE val2 IS NULL;
-- check array support
create foreign table gotest3 (val integer[], val2 text[]) server ch_fdw options (table 'gotest2');
SELECT * FROM gotest3;
-- check array equality (different then IN operator, this is treated as const expr)
SELECT val FROM gotest3 WHERE val = ARRAY[1, 2];
-- simple aggregate functions
create foreign table gotest4 (val numeric, val2 numeric) server ch_fdw options (table 'gotest3');
SELECT sum(val),max(val2), min(val2) from gotest4;
SELECT sum(val+5)+2 from gotest4 group by val2;
select sum(val2) from gotest4 group by val having sum(val2) > 1 AND sum(val) > 2;
-- only conditions are evaluated , order by is evaluated locally
select * from gotest4 WHERE val2 > 1 and val > 2 order by val;
-- group by extract example
select extract(month from val5) as month, sum(val) from gotest group by month  ;