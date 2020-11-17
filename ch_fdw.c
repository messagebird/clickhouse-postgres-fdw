#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "ch_fdw.h"

PG_MODULE_MAGIC;

extern Datum ch_fdw_handler(PG_FUNCTION_ARGS);
extern Datum ch_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(ch_fdw_handler);
PG_FUNCTION_INFO_V1(ch_fdw_validator);

/*
 * Foreign-data wrapper handler function
 */
Datum
ch_fdw_handler(PG_FUNCTION_ARGS)
{
  FdwRoutine *fdwroutine = makeNode(FdwRoutine);
  
  fdwroutine->GetForeignRelSize = chGetForeignRelSize;
  fdwroutine->GetForeignPaths = chGetForeignPaths;
  fdwroutine->GetForeignPlan = chGetForeignPlan;
  fdwroutine->ExplainForeignScan = chExplainForeignPlan;
  fdwroutine->BeginForeignScan = chBeginForeignScan;
  fdwroutine->IterateForeignScan = chIterateForeginScan;
  fdwroutine->ReScanForeignScan = chReScanForeignScan;
  fdwroutine->EndForeignScan = chEndForeignScan;
  fdwroutine->AnalyzeForeignTable = chAnalyzeForeignTable;
  fdwroutine->GetForeignUpperPaths = chGetForeignUpperPaths;

  PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER
 * USER MAPPING or FOREIGN TABLE that uses hello_fdw.
 */
Datum
ch_fdw_validator(PG_FUNCTION_ARGS)
{
  /* no-op */
  PG_RETURN_VOID();
}