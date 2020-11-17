# Is ch_fdw in Go a good idea ?
In order to properly understand the FDW's working and further extension, it is required to understand Postgres internals and it's FDW API.
Further a brief understanding of database design theory is also needed.
If the teams doesn't have such expertise beforehand, this can be a major bottleneck in providing a production grade interface between Foreign DB(like Clickhouse) and PG.
It shouldn't be the case that developers fear the code because it is too magical and they don't understand the inner workings.

Next, if one is assumed to have an understanding, there are the challenges of keeping the C to Go interface calls manageable. See [CGo performance penalties](https://www.cockroachlabs.com/blog/the-cost-and-complexity-of-cgo/) and keeping the codebase sane by keeping the C code at minimum.
We somehow need to provide a balance between the two.

Further challenges can be:
 * We can try to keep much of the code in GO (involving C interface functions) but we also need to be careful while passing memory chunks from Go to C.
   Any `Go.CString(...)`s aren't claimed back via Go runtime, although in case of C structs PG runtime promises to claim back the memory allocated by palloc as soon as the transaction ends. There are some constructs where we might need to touch PG's HeapTuple memory allocators and manually make sure we free them after use.
 * New collaborators will need to learn tricks around the converting C structs/datatypes to Go ones(and vice versa) and marshalling/unmarshalling results/arguments of PG C functions.

A very extreme way is, To try to keep most of FDW stages in C with 'deparsing and type conversions from Go types to C types' and 'Clickhouse query formation' in Go, but if the teams lack expertise in C, this may not be a favourable option.