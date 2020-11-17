
#include "postgres.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/tupdesc.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "optimizer/tlist.h"

typedef struct ChFdwExecutionState
{
	uint64 tok;
	// List of attr numbers we need to fetch from the remote server.
	List  *retrieved_attrs;
} ChFdwExecutionState;

typedef struct ChFdwRelationInfo {
	// baserestrictinfo clauses, broken down into safe and unsafe subsets
	List	   *remote_conds;
	List	   *local_conds;
	// Estimated size and cost for a scan or join.
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	// Costs excluding costs for transferring data from the foreign server
	Cost		rel_startup_cost;
	Cost		rel_total_cost;
	// Options extracted from catalogs.
	bool		use_remote_estimate;
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	// Bitmap of attr numbers we need to fetch from the remote server.
	Bitmapset  *attrs_used;
	// Join information
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType	jointype;
	List	   *joinclauses;
	// Cached catalog information
	ForeignTable *table;
	ForeignServer *server;
	/* Grouping information */
	List	   *grouped_tlist;
}ChFdwRelationInfo;

DefElem* cellGetDef(ListCell *n);
void elog_notice(char* string);
void elog_info(char* string);
void elog_error(char* string);
void elog_debug(char* string);

void deparse_column_ref(StringInfo buf, int varno, int varattno, PlannerInfo *root);
void extract_target_columns(PlannerInfo *root, RelOptInfo *foreignrel, List **retrieved_attrs, StringInfo data);
void extract_tablename(PlannerInfo *root, RelOptInfo *baserel, StringInfo data);
void *wrapper_list_make3(Value *data, Value *data2, List *data3);
void *wrapper_list_make1(void *data);
void *wrapper_list_make4(Value *data, Value *data2, Value *data3, List *data4);
void *wrapper_list_nth(const List *list, int n);
void *wrapper_strVal(Value *item);
int wrapper_intVal(Value *item);
ListCell* wrapper_lnext(List *List, ListCell *lc);
int wrapper_lfirst_int(ListCell *lc);
void *wrapper_lfirst(ListCell *lc);
List *wrapper_lappend(List *list, void *datum);
void* wrapper_TupleDescAttr(TupleDesc t, int i);
HeapTuple wrapper_SearchSysCache1Oid(Datum key);
HeapTuple wrapper_SearchSysCache1(Oid id, Datum key1);
Datum wrapper_ObjectIdGetDatum(Oid id);
bool wrapper_HeapTupleIsValid(HeapTuple tuple);
void *wrapper_GETSTRUCT(HeapTuple tuple);
Datum wrapper_CStringGetDatum(char * str);
Datum wrapper_OidFunctionCall3(regproc func, Datum value, Datum id, Datum mod);
Datum wrapper_Int32GetDatum(int val);
Datum wrapper_Int64GetDatum(int64 val);
Datum wrapper_Uint64GetDatum(uint64 val);
Datum wrapper_Float4GetDatum(float val);
Datum wrapper_Float8GetDatum(double val);
NodeTag wrapper_nodeTag(Node* node);
RangeTblEntry * wrapper_rt_fetch(Index i, List * rangeTable);
unsigned int wrapper_get_pathtarget_sortgroupref(PathTarget * target, int i);
/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation derives from something else */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
} foreign_loc_cxt;

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
bool foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt);
/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
is_foreign_expr(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Expr *expr);