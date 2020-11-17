#include "postgres.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/tupdesc.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_namespace.h"
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
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "optimizer/clauses.h"
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
#include "ch_helpers.h"

DefElem* cellGetDef(ListCell *n) { return (DefElem*)n->ptr_value; }
void elog_notice(char* string) {
    elog(NOTICE, string, "");
}
void elog_info(char* string) {
    elog(INFO, string, "");
}
void elog_error(char* string) {
    elog(ERROR, string, "");
}
void elog_debug(char* string) {
    elog(DEBUG1, string, "");
}

void deparse_column_ref(StringInfo buf, int varno, int varattno, PlannerInfo *root) {
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;
	// varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR.
	Assert(!IS_SPECIAL_VARNO(varno));
	// Get RangeTblEntry from array in PlannerInfo.
	rte = planner_rt_fetch(varno, root);

	// If it's a column of a foreign table, and it has the column_name FDW
	// option, use that value.
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	// If it's a column of a regular table or it doesn't have column_name FDW
	// option, use attribute name.

	if (colname == NULL)
		colname = get_attname(rte->relid, varattno,false);
    appendStringInfoString(buf, colname);
}
void extract_target_columns(PlannerInfo *root, RelOptInfo *foreignrel, List **retrieved_attrs, StringInfo data) {
    ChFdwRelationInfo *fpinfo = (ChFdwRelationInfo *) foreignrel->fdw_private;
    RangeTblEntry *rte;
    Relation	rel;
    bool		have_wholerow;
    int			i;
    TupleDesc	tupdesc;
    initStringInfo(data);
	bool first;

    // only supporting RELOPT_BASEREL at the moment
    // For a base relation fpinfo->attrs_used gives the list of columns
    // required to be fetched from the foreign server.
	rte = planner_rt_fetch(foreignrel->relid, root);

    // Core code already has some lock on each rel being planned, so we
    // can use NoLock here.
    rel = table_open(rte->relid, NoLock);
	tupdesc = RelationGetDescr(rel);

    // If there's a whole-row reference, we'll need all the columns.
    have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
                                    fpinfo->attrs_used);
    *retrieved_attrs = NIL;
	first = true;
    for (i = 1; i <= tupdesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
        // Ignore dropped attributes.
        if (attr->attisdropped)
            continue;
        if (have_wholerow ||
            bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
                            fpinfo->attrs_used))
        {
			if (!first) {
				appendStringInfoString(data, ",");
			}
            deparse_column_ref(data, foreignrel->relid, i, root);
			first = false;
            *retrieved_attrs = lappend_int(*retrieved_attrs, i);
        }
    }
    table_close(rel, NoLock);
}
void extract_tablename(PlannerInfo *root, RelOptInfo *baserel, StringInfo data) {
    ForeignTable *table;
    const char *relname = NULL;
    const char *dbname = NULL;
    ListCell   *lc = NULL;
    RangeTblEntry *rte;
    Relation	rel;
    initStringInfo(data);
    if (bms_num_members(baserel->relids) > 1) {
        elog_error("alias not supported");
    }
	rte = planner_rt_fetch(baserel->relid, root);

    // Core code already has some lock on each rel being planned, so we
    // can use NoLock here.
    rel = table_open(rte->relid, NoLock);
    // obtain additional catalog information.
    table = GetForeignTable(RelationGetRelid(rel));

	// Use value of FDW options if any, instead of the name of object itself.
    foreach(lc, table->options)
    {
        DefElem    *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, "table") == 0)
            relname = defGetString(def);
        if (strcmp(def->defname, "db") == 0)
            dbname = defGetString(def);
    }
    // if no option passed for table, we'll use the relation name itself
    if (relname == NULL) {
        relname = RelationGetRelationName(rel);
    }
    // if db is passed then make table name with db, else direct tablename
    if (dbname != NULL) {
        appendStringInfo(data, "%s.%s", dbname, relname);
    } else {
        appendStringInfo(data, "%s", relname);
    }
    table_close(rel, NoLock);
}
void *wrapper_list_make3(Value *data, Value *data2, List *data3)
{
    return list_make3(data, data2, data3);
}
void *wrapper_list_make1(void *data)
{
    return list_make1(data);
}
void *wrapper_list_nth(const List *list, int n) {
    return list_nth(list, n);
}
void *wrapper_strVal(Value *item){
    return item->val.str;
}
int wrapper_intVal(Value *item){
    return item->val.ival;
}
ListCell* wrapper_lnext(List *l, ListCell *lc){
   return lnext(l, lc);
}
int wrapper_lfirst_int(ListCell *lc){
   return lfirst_int(lc);
}
void *wrapper_lfirst(ListCell *lc){
   return lfirst(lc);
}
List *wrapper_lappend(List *list, void *datum) {
    return lappend(list, datum);
}
void* wrapper_TupleDescAttr(TupleDesc t, int i){
	return TupleDescAttr(t, i);
}
HeapTuple wrapper_SearchSysCache1Oid(Datum key1) {
	return SearchSysCache1(TYPEOID, key1);
}
HeapTuple wrapper_SearchSysCache1(Oid id, Datum key1) {
	return SearchSysCache1(id, key1);
}
Datum wrapper_ObjectIdGetDatum(Oid id){
	return ObjectIdGetDatum(id);
}
bool wrapper_HeapTupleIsValid(HeapTuple tuple){
	return HeapTupleIsValid(tuple);
}
void *wrapper_GETSTRUCT(HeapTuple tuple) {
	return GETSTRUCT(tuple);
}
Datum wrapper_CStringGetDatum(char * str){
	return CStringGetDatum(str);
}
Datum wrapper_OidFunctionCall3(regproc func, Datum value, Datum id, Datum mod) {
	return OidFunctionCall3(func, value, id, mod);
}
Datum wrapper_Int32GetDatum(int val) {
	return Int32GetDatum(val);
}
Datum wrapper_Int64GetDatum(int64 val) {
	return Int64GetDatum(val);
}
Datum wrapper_Uint64GetDatum(uint64 val) {
	return UInt64GetDatum(val);
}
Datum wrapper_Float4GetDatum(float val) {
    return Float4GetDatum(val);
}
Datum wrapper_Float8GetDatum(double val) {
    return Float8GetDatum(val);
}

NodeTag wrapper_nodeTag(Node* node){
    return nodeTag(node);
}

void *wrapper_list_make4(Value *data, Value *data2, Value *data3, List *data4) {
    return list_make4(data, data2, data3, data4);
}

unsigned int wrapper_get_pathtarget_sortgroupref(PathTarget * target, int i) {
	return get_pathtarget_sortgroupref(target, i);
}

RangeTblEntry * wrapper_rt_fetch(Index i, List * rangeTable) {
	return rt_fetch(i, rangeTable);
}
// These foreign_exprs detections were taken from sqlite_fdw: https://github.com/pgspider/sqlite_fdw
/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
is_foreign_expr(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	ChFdwRelationInfo *fpinfo = (ChFdwRelationInfo *) (baserel->fdw_private);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;

	/*
     * 
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (baserel->reloptkind == RELOPT_UPPER_REL)
		glob_cxt.relids = fpinfo->outerrel->relids; // return false;
	else
        glob_cxt.relids = baserel->relids;
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	if (!foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt)) {
		return false;
	}

	/*
	 * If the expression has a valid collation that does not arise from a
	 * foreign var, the expression can not be sent over.
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (contain_mutable_functions((Node *) expr))
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else format_type might incorrectly fail to schema-qualify their names.
 * (This could be fixed with some changes to format_type, but for now there's
 * no need.)  Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
static bool
is_builtin(Oid oid)
{
	return (oid < FirstBootstrapObjectId);
}


/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (which we approximate
 * as being built-in), and that all collations used in the expression derive
 * from Vars of the foreign table.  Because of the latter, the logic is
 * pretty close to assign_collations_walker() in parse_collate.c, though we
 * can assume here that the given expression is valid.
 */
bool
foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	foreign_loc_cxt inner_cxt;
	Oid			collation;
	FDWCollateState state;
	HeapTuple	tuple;
	Form_pg_operator form;
	char	   *cur_opname;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	// elog(INFO, "in with walker %d", nodeTag(node));
	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;
	switch (nodeTag(node))
	{
		case T_Var:
			// elog(INFO, "got T_VAR");
			{
				Var		   *var = (Var *) node;

				/*
				 * If the Var is from the foreign table, we consider its
				 * collation (if any) safe to use.  If it is from another
				 * table, we treat its collation the same way as we would a
				 * Param's collation, ie it's not safe for it to have a
				 * non-default collation.
				 */
				if (bms_is_member(var->varno, glob_cxt->relids) &&
					var->varlevelsup == 0)
				{
					/* Var belongs to foreign table */

					/*
					 * System columns other than ctid and oid should not be
					 * sent to the remote, since we don't make any effort to
					 * ensure that local and remote values match (tableoid, in
					 * particular, almost certainly doesn't match).
					 */
					if (var->varattno < 0 &&
						var->varattno != SelfItemPointerAttributeNumber &&
						var->varattno != TableOidAttributeNumber){
						return false;

						}

					/* Else check the collation */
					collation = var->varcollid;
					state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
				}
				else
				{
					/* Var belongs to some other table */
					collation = var->varcollid;
					if (collation == InvalidOid ||
						collation == DEFAULT_COLLATION_OID)
					{
						/*
						 * It's noncollatable, or it's safe to combine with a
						 * collatable foreign Var, so set state to NONE.
						 */
						state = FDW_COLLATE_NONE;
					}
					else
					{
						/*
						 * Do not fail right away, since the Var might appear
						 * in a collation-insensitive context.
						 */
						state = FDW_COLLATE_UNSAFE;
					}
				}
			}
			break;
		case T_Const:
			// elog(INFO, "got T_Const");
			{
				Const	   *c = (Const *) node;

				/* we cannot handle interval type */
				if (c->consttype == INTERVALOID){
					return false;
				}

				/*
				 * If the constant has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr;
				 * either way, it's unsafe to send to the remote.
				 */
				if (c->constcollid != InvalidOid &&
					c->constcollid != DEFAULT_COLLATION_OID)
					return false;

				/* Otherwise, we can consider that it doesn't set collation */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_CaseTestExpr:
			// elog(INFO, " got T_CaseTestExpr");
			{
				CaseTestExpr *c = (CaseTestExpr *) node;

				/*
				 * If the expr has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr;
				 * either way, it's unsafe to send to the remote.
				 */
				if (c->collation != InvalidOid &&
					c->collation != DEFAULT_COLLATION_OID)
					return false;

				/* Otherwise, we can consider that it doesn't set collation */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_Param:
			// elog(INFO, " got T_Param");
            // no param support yet
            return false;
			{
				Param	   *p = (Param *) node;

				/*
				 * Collation rule is same as for Consts and non-foreign Vars.
				 */
				collation = p->paramcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *func = (FuncExpr *) node;
				char	   *opername = NULL;
				Oid			schema;

				/* get function name and schema */
				tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->funcid));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for function %u", func->funcid);
				}
				opername = pstrdup(((Form_pg_proc) GETSTRUCT(tuple))->proname.data);
				schema = ((Form_pg_proc) GETSTRUCT(tuple))->pronamespace;
				ReleaseSysCache(tuple);

				// elog(INFO, "got func %s",opername );
				/* ignore functions in other than the pg_catalog schema */
				if (schema != PG_CATALOG_NAMESPACE)
					return false;

				/* these function can be passed to CH */
                // any additional functions that needs to be supported
				// should be added here
				// note that these funcnames are PG ones, mapping to CH equivalents
				// happens at deparsing stage
				if (!(strcmp(opername, "abs") == 0
					  || strcmp(opername, "length") == 0
					  || strcmp(opername, "lower") == 0
					  || strcmp(opername, "replace") == 0
					  || strcmp(opername, "round") == 0
					  || strcmp(opername, "substr") == 0
					  || strcmp(opername, "date_part") == 0
					  || strcmp(opername, "timestamp") == 0
					  || strcmp(opername, "upper") == 0))
				{
					return false;
				}

				if (!foreign_expr_walker((Node *) func->args,
										 glob_cxt, &inner_cxt)){
					return false;
				}


				/*
				 * If function's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (func->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if(func->inputcollid == DEFAULT_COLLATION_OID)
					 /* Default Collation OID means No collation */ ;
				else if (( inner_cxt.state != FDW_COLLATE_SAFE  && inner_cxt.state != FDW_COLLATE_NONE)||
						 func->inputcollid != inner_cxt.collation) {

					// elog(INFO, "unsafe collation %d %d %d ", inner_cxt.state, inner_cxt.collation, func->inputcollid );
					return false;
				}

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = func->funccollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_OpExpr:
		case T_NullIfExpr:
			// elog(INFO, "got T_OpExpr/T_NullIfExpr");
			{
				OpExpr	   *oe = (OpExpr *) node;

				/*
				 * Similarly, only built-in operators can be sent to remote.
				 * (If the operator is, surely its underlying function is
				 * too.)
				 */
				if (!is_builtin(oe->opno))
					return false;

				tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oe->opno));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for operator %u", oe->opno);
				form = (Form_pg_operator) GETSTRUCT(tuple);

				/* opname is not a SQL identifier, so we should not quote it. */
				cur_opname = NameStr(form->oprname);

				/* ILIKE cannot be pushed down yet.. */
                // TODO: Review ILIKE support in CH
				if (strcmp(cur_opname, "~~*") == 0 || strcmp(cur_opname, "!~~*") == 0)
				{
					ReleaseSysCache(tuple);
					return false;
				}
				ReleaseSysCache(tuple);

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Result-collation handling is same as for functions */
				collation = oe->opcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ScalarArrayOpExpr:
			// elog(INFO, "got T_ScalarArrayOpExpr");
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

				/*
				 * Again, only built-in operators can be sent to remote.
				 */
				if (!is_builtin(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_RelabelType:
			// elog(INFO, "T_RelabelType");
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!foreign_expr_walker((Node *) r->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * RelabelType must not introduce a collation not derived from
				 * an input foreign Var.
				 */
				collation = r->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_BoolExpr:
			// elog(INFO, "got T_BoolExpr");
			{
				BoolExpr   *b = (BoolExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_NullTest:
			// elog(INFO, "got T_NullTest");
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) nt->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_List:
			// elog(INFO, "got T_list");
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */
				collation = inner_cxt.collation;
				state = inner_cxt.state;

				/* Don't apply exprType() to the list. */
				check_type = false;
			}
			break;
		case T_CoalesceExpr:
			// elog(INFO, " got  T_CoalesceExpr");
			{
				CoalesceExpr *coalesce = (CoalesceExpr *) node;
				ListCell   *lc;

				if (list_length(coalesce->args) < 2)
					return false;

				/* Recurse to each argument */
				foreach(lc, coalesce->args)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt))
						return false;
				}
			}
			break;
		case T_CaseExpr:
			// elog(INFO, "got  T_CoalesceExpr");
			{
				ListCell   *lc;

				/* Recurse to condition subexpressions. */
				foreach(lc, ((CaseExpr *) node)->args)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt))
						return false;
				}
			}
			break;
		case T_CaseWhen:
			// elog(INFO, "got T_CaseWhen");
			{
				CaseWhen   *whenExpr = (CaseWhen *) node;

				/* Recurse to condition expression. */
				if (!foreign_expr_walker((Node *) whenExpr->expr,
										 glob_cxt, &inner_cxt))
					return false;
				/* Recurse to result expression. */
				if (!foreign_expr_walker((Node *) whenExpr->result,
										 glob_cxt, &inner_cxt))
					return false;
				/* Don't apply exprType() to the case when expr. */
				check_type = false;
			}
			break;
		case T_Aggref:
			// elog(INFO, " got T_Aggref");
			{

				Aggref	   *agg = (Aggref *) node;
				ListCell   *lc;
				char	   *opername = NULL;
				Oid			schema;

				/* get function name and schema */
				tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(agg->aggfnoid));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for function %u", agg->aggfnoid);
				}
				opername = pstrdup(((Form_pg_proc) GETSTRUCT(tuple))->proname.data);
				schema = ((Form_pg_proc) GETSTRUCT(tuple))->pronamespace;
				ReleaseSysCache(tuple);

				/* ignore functions in other than the pg_catalog schema */
				if (schema != PG_CATALOG_NAMESPACE)
					return false;

				// these function can be passed to CH
                // any additional functions that needs to be supported
				// should be added here
				// note that these funcnames are PG ones, mapping to CH equivalents
				// happens at deparsing stage
				// TODO: review this func list

				if (!(strcmp(opername, "sum") == 0
					  || strcmp(opername, "avg") == 0
					  || strcmp(opername, "max") == 0
					  || strcmp(opername, "min") == 0
					  || strcmp(opername, "count") == 0))
				{
					return false;
				}


				/* Not safe to pushdown when not in grouping context */
				if (glob_cxt->foreignrel->reloptkind != RELOPT_UPPER_REL)
					return false;

				/* Only non-split aggregates are pushable. */
				if (agg->aggsplit != AGGSPLIT_SIMPLE)
					return false;

				/*
				 * Recurse to input args. aggdirectargs, aggorder and
				 * aggdistinct are all present in args, so no need to check
				 * their shippability explicitly.
				 */
				foreach(lc, agg->args)
				{
					Node	   *n = (Node *) lfirst(lc);

					/* If TargetEntry, extract the expression from it */
					if (IsA(n, TargetEntry))
					{
						TargetEntry *tle = (TargetEntry *) n;

						n = (Node *) tle->expr;
					}

					if (!foreign_expr_walker(n, glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * For aggorder elements, check whether the sort operator, if
				 * specified, is shippable or not.
				 */
				// no ORDER BY support yet
				if (agg->aggorder)
				{
					return false;
				}

				/* Check aggregate filter */
				if (!foreign_expr_walker((Node *) agg->aggfilter,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If aggregate's input collation is not derived from a
				 * foreign Var, it can't be sent to remote.
				 */
				if (agg->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 agg->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = agg->aggcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ArrayExpr:
		case T_DistinctExpr:
			// elog(INFO, "got T_ArrayExpr/T_DistinctExpr");
			/* IS DISTINCT FROM */
			return false;
		default:
			// elog(INFO, "unknown %d", nodeTag(node));
			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

	/*
	 * If result type of given expression is not built-in, it can't be sent to
	 * remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !is_builtin(exprType(node))) {
		return false;
	}

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
			case FDW_COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case FDW_COLLATE_SAFE:
				if (collation != outer_cxt->collation)
				{
					/*
					 * Non-default collation always beats default.
					 */
					if (outer_cxt->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						outer_cxt->collation = collation;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Conflict; show state as indeterminate.  We don't
						 * want to "return false" right away, since parent
						 * node might not care about collation.
						 */
						outer_cxt->state = FDW_COLLATE_UNSAFE;
					}
				}
				break;
			case FDW_COLLATE_UNSAFE:
				/* We're still conflicted ... */
				break;
		}
	}
	/* It looks OK */
	return true;
}