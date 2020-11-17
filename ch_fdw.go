/*
We want to implement CH API functions which should allow us to query Clickhouse(CH).
However, Postgres(PG) only have a FDW API in C.
This means the CH API functions needs to be callable from the C interface but implemented in GO.
Only option is exported Go functions via CGO https://golang.org/cmd/cgo/#hdr-C_references_to_Go

PG extension entry point will be defined in `ch_fdw.c`, which implements a handler and a validator.
Handler returns a struct of function pointers that are called by PG planner, executor and so forth.
These functions are to be implemented in GO. These are marked with `export funcName`.
*/
package main

/*
#cgo darwin CFLAGS: -I/usr/local/include/postgresql/internal/ -I/usr/local/include/postgresql/server
#cgo linux CFLAGS: -I/usr/include/postgresql/13/server -I/usr/include/postgresql/internal
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "ch_helpers.h"
*/
import "C"

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// set of options passed by FDW options during table creation
type Options map[string]string

// Oid is Postgres Internal Object ID
type Oid uint

type elogLevel int

const (
	noticeLevel elogLevel = iota
	infoLevel
	debugLevel
)

type elog struct {
	Level elogLevel
}

// we need this because wrapping elog_error with standard go logger hangs up on subsequent calls
type errorLogger struct {
}

func (e *errorLogger) Println(str string) {
	C.elog_error(C.CString(str))
}

func (e *errorLogger) Printf(str string, args ...interface{}) {
	C.elog_error(C.CString(fmt.Sprintf(str, args...)))
}

// we need to maintain execution state
// when starting foreign scan i.e. while BeginForeignScan
// but passing Go Pointers to C is tricky(https://golang.org/cmd/cgo/#hdr-Passing_pointers)
// Go forbids passing Go {pointers to C which itself contains a Go Pointers for example maps, slices etc.
// in FDW case we want to save pointer to the CH connection, current row index
// in the execution state.
// We need to make a design choice on how to do this i.e. map C Exec state to Go Exec state
// simplest one is via a shared integer btwn go and C
type fdwExecState struct {
	ch            *chserver
	targetColumns []string
	tableName     string
	query         *Query
	rows          *sql.Rows
}

var (
	// continously increment this index on calls to BeginForeignScan (which will always mean a new scan)
	sessionIndex uint64
	// store link btwn sessionIndex and the exec state associated with it
	stateMap    sync.Map
	infoLogger  = log.New(&elog{Level: infoLevel}, "", 0)
	debugLogger = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)
	errLogger   = &errorLogger{}
)

// save link to the passed exec state
// and return the current index
func saveState(state *fdwExecState) uint64 {
	atomic.AddUint64(&sessionIndex, 1)
	stateMap.Store(sessionIndex, state)
	return sessionIndex
}

//return the stored state at passed index
func getState(index uint64) *fdwExecState {
	state, ok := stateMap.Load(index)
	if !ok {
		C.elog_error(C.CString(fmt.Sprintf("found nil go exec state for passed %v", index)))
	}
	return state.(*fdwExecState)
}

// delete state at passed index
func deleteState(index uint64) {
	stateMap.Delete(index)
}

// we need to parse column names from the passed
// foreign relation
type relation struct {
	ID      Oid
	IsValid bool
	Attr    *tupleDesc
}

type tupleDesc struct {
	TypeID  Oid
	TypeMod int
	HasOid  bool
	Attrs   []attr // columns
}

type attr struct {
	Name    string
	Type    Oid
	NotNull bool
}

func (e *elog) Write(p []byte) (n int, err error) {

	switch e.Level {
	case noticeLevel:
		C.elog_notice(C.CString(string(p)))
	case infoLevel:
		// use TrimSpace, to remove extra new line
		C.elog_info(C.CString(strings.TrimSpace(string(p))))
	case debugLevel:
		C.elog_debug(C.CString(strings.TrimSpace(string(p))))
	}
	return len(p), nil
}

//getFTableOptions builds `Options` from passed Oid for a table
func getFTableOptions(id Oid) Options {
	f := C.GetForeignTable(C.Oid(id))
	return getOptions(f.options)
}

//getFServerOptions builds `Options` from passed Oid for a server
func getFServerOptions(id Oid) Options {
	f := C.GetForeignServer(C.Oid(id))
	return getOptions(f.options)
}

//getOptions builds `Options` from passed `List`
func getOptions(opts *C.List) Options {
	m := make(Options)
	// iterate over `List` structs defined by PG API
	for it := C.list_head(opts); it != nil; it = C.wrapper_lnext(opts, it) {
		// type of `it` is *C.ListCell
		// original data is of type `void *`
		// cast it to *C.DefElem
		// but we need to use glue C code
		// as the underlying ListCell has data in `union`
		// which isn't directly accessible via cgo
		el := C.cellGetDef(it)
		name := C.GoString(el.defname)
		val := C.GoString(C.defGetString(el))
		m[name] = val
	}
	return m
}

//export chGetForeignRelSize
func chGetForeignRelSize(root *C.PlannerInfo, baserel *C.RelOptInfo, foreigntableid C.Oid) {

	var (
		// pass fdw_private information btwn stages
		fpinfo *C.ChFdwRelationInfo
		ri     *C.RestrictInfo
	)
	// this should update baserel.rows to return estimated number of row to be used in scans

	// tableOpts := getFTableOptions(Oid(foreigntableid))
	// serverOpts := getFServerOptions(Oid(fTable.serverid))
	// infoLogger.Printf("passed options from table %v", tableOpts)
	// infoLogger.Printf("passed options from server %v", serverOpts)

	// infoLogger.Println("in chGetForeignRelSize")
	fTable := C.GetForeignTable(foreigntableid)
	fpinfo = (*C.ChFdwRelationInfo)(unsafe.Pointer(C.palloc0(C.sizeof_struct_ChFdwRelationInfo)))
	baserel.fdw_private = unsafe.Pointer(fpinfo)

	fpinfo.table = fTable
	fpinfo.server = C.GetForeignServer(fTable.serverid)

	// baserestrictinfo is a list of RestrictInfo structs which contains any restriction(filter) clauses that PG query parser has determined.
	// We iterate over the list using PG defined functions for lists tlist.h.
	// It's cumbersome because there is a foreach C macro which can make this look simpler.
	// But CGO doesn't allow directly calling C #define macros.
	// Some of the list functions are however defined as static inline functions in C code and not C macro so that can be used directly.
	// As workaround, we define wrapper functions in C (ch_helpers.c) which provide C macros as functions that we can use in GO.
	// As we iterate over the list, we pull out the cell from list and cast it to RestrictInfo.
	// Rationale for doing this is because we want to see which RestrictInfo(filter) clauses can be sent to remote server (Clickhouse) and which can't.
	// is_foreign_expr evaluates the Expr inside RestrictInfo.clause and reports if they can be or not.
	// We keep all the fdwstate inside fpinfo, which isn't touched by PG code and we can pass it around different stages to manage states like parsed query clauses, table names etc.
	// In the next stage, remote_conds will be parsed and prepared to be sent on remote and local_conds will be returned as is to PG.
	// local_conds will be evaluated by PG internally after all data is collected.
	for cell := C.list_head(baserel.baserestrictinfo); cell != nil; cell = C.wrapper_lnext(baserel.baserestrictinfo, cell) {

		ri = (*C.RestrictInfo)(unsafe.Pointer(C.wrapper_lfirst(cell)))
		if bool(C.is_foreign_expr(root, baserel, ri.clause)) {
			fpinfo.remote_conds = C.wrapper_lappend(fpinfo.remote_conds, unsafe.Pointer(ri))
		} else {
			fpinfo.local_conds = C.wrapper_lappend(fpinfo.local_conds, unsafe.Pointer(ri))
		}
	}
	//from PG docs:
	//Find all the distinct attribute numbers present in an expression tree,
	//and add them to the initial contents of *varattnos.
	C.pull_varattnos(
		(*C.Node)(unsafe.Pointer(baserel.reltarget.exprs)),
		baserel.relid,
		&fpinfo.attrs_used)

	//Iterate over local conds that we extracted above
	//and make sure that any attrs that are not in target list
	//but are required for local computation on PG
	//are fetched from CH server
	for cell := C.list_head(fpinfo.local_conds); cell != nil; cell = C.wrapper_lnext(fpinfo.local_conds, cell) {
		rinfo := (*C.RestrictInfo)(unsafe.Pointer(C.wrapper_lfirst(cell)))
		C.pull_varattnos(
			(*C.Node)(unsafe.Pointer(rinfo.clause)),
			baserel.relid,
			&fpinfo.attrs_used,
		)
	}

	baserel.rows = 1000 // constant for the time being until we implement get_estimate
	baserel.tuples = 1000
}

//export chGetForeignPaths
func chGetForeignPaths(
	root *C.PlannerInfo, baserel *C.RelOptInfo, foreigntableid C.Oid) {

	var (
		pathkeys       *C.List
		required_outer *C.Bitmapset // *C.Relids
		fdw_outerpath  *C.Path
		fdw_private    *C.List
	)

	C.add_path(baserel,
		(*C.Path)(unsafe.Pointer(C.create_foreignscan_path(
			root,
			baserel,
			baserel.reltarget,
			baserel.rows,
			C.double(10),   // startup_cost
			C.double(1000), // total
			pathkeys,       // no pathkeys
			required_outer, // no outer rel either
			fdw_outerpath,  // no extra plan
			fdw_private,
		))))
}

//export chGetForeignPlan
func chGetForeignPlan(
	root *C.PlannerInfo, baserel *C.RelOptInfo, foreigntableid C.Oid,
	bestpath *C.ForeignPath, tlist *C.List, scan_clauses *C.List, outer_plan *C.Plan) *C.ForeignScan {

	// Note: This currently only works with
	// baserel.reloptkind == RELOPT_BASEREL/RELOPT_UPPER_REL
	// i.e. no join relations are supported
	var (
		fdwprivate *C.List
		// extract the passed down fdw_private
		fpinfo *C.ChFdwRelationInfo
		// a target list that has the columns specified in fpinfo.attrs_used.
		retrievedAttrs *C.List
		sqlData        C.StringInfoData
		tableData      C.StringInfoData
		conditionsData C.StringInfoData
		groupByData    C.StringInfoData
		havingData     C.StringInfoData
		localExprs     *C.List
		remoteExprs    *C.List
		targetList     *C.List
		// scan rel is same as baserel in case of BASEREL but
		// will be underlying inner REL in case of UPPERREL or JOIN REL
		scanRel   *C.RelOptInfo
		scanRelID = baserel.relid
		// From `plannodes.h`
		// fdw_recheck_quals should contain any quals which the core system passed to
		// the FDW but which were not added to scan.plan.qual; that is, it should
		// contain the quals being checked remotely.  This is needed for correct
		// behavior during EvalPlanQual rechecks.
		fdwRecheckQuals *C.List
		fdwScanTlist    *C.List
	)
	// infoLogger.Println("in chGetForeignPlan")

	fpinfo = (*C.ChFdwRelationInfo)(unsafe.Pointer(baserel.fdw_private))

	// Separate scan_clauses into those that can be executed remotely and those which can't.
	// We already have `local_conds` and `remote_conds` being passed from previous stage
	// in fpinfo.
	// Join clauses will need to be made sure that they are remote safe.
	// This code should do the same as "extract_actual_clauses(scan_clauses, false)"
	// except for the additional decision about remote versus local execution.
	if baserel.reloptkind == C.RELOPT_BASEREL || baserel.reloptkind == C.RELOPT_OTHER_MEMBER_REL {

		// for simple relations, scanRel is same as baserel
		scanRel = baserel

		for cell := C.list_head(scan_clauses); cell != nil; cell = C.wrapper_lnext(scan_clauses, cell) {
			// cast to RestrictInfo (relation.h) from the list of scan clauses
			rinfo := (*C.RestrictInfo)(unsafe.Pointer(C.wrapper_lfirst(cell)))

			// we assume `scan_clauses` is made of RestrictInfo only
			// or implement Assert(IsA(rinfo, RestrictInfo))

			// ignore `pseudoconstant`
			// FROM PG `relation.h`:
			// The pseudoconstant flag is set true if the clause contains no Vars of
			// the current query level and no volatile functions.
			if rinfo.pseudoconstant {
				continue
			}

			// now check if the pulled out `rinfo` is present in our
			// parsed `fpinfo.remote_conds` or `fpinfo.local_conds`
			// and extract the list of `remoteExprs` and `localExprs`
			// accordingly
			if C.list_member_ptr(fpinfo.remote_conds, unsafe.Pointer(rinfo)) {
				remoteExprs = C.wrapper_lappend(remoteExprs, unsafe.Pointer(rinfo.clause))
			} else if C.list_member_ptr(fpinfo.local_conds, unsafe.Pointer(rinfo)) {
				localExprs = C.wrapper_lappend(localExprs, unsafe.Pointer(rinfo.clause))
			} else {
				// this means that scan_clauses has smth
				// which we can't classify in the previous stage or we didn't check
				// check again if the expr is safe to execute on remote
				if bool(C.is_foreign_expr(root, scanRel, rinfo.clause)) {
					remoteExprs = C.wrapper_lappend(remoteExprs, unsafe.Pointer(rinfo.clause))
				} else {
					// just add the clause to local_conds then
					// PG planner/executor will take care of it
					localExprs = C.wrapper_lappend(localExprs, unsafe.Pointer(rinfo.clause))
				}
			}

			fdwRecheckQuals = remoteExprs
		}

		// extract column names
		// put them into retreivedAttrs *C.List
		// this is somewhat complicated
		C.extract_target_columns(root, scanRel, &retrievedAttrs, &sqlData)
		// we have target columns to be fetched in sqlData.data
		scan_clauses = C.extract_actual_clauses(scan_clauses, false)
		// check for UPPER_REL
	} else if baserel.reloptkind == C.RELOPT_UPPER_REL {

		// for UPPER_REL, use the underlying INNER rel(which is the actual relation) that we pass in fpinfo
		scanRel = fpinfo.outerrel
		// set scan relid to 0 (no scans needed)
		scanRelID = 0

		// For both of the RELs, baserestrictinfo should be NIL
		if scan_clauses != nil {
			errLogger.Println("unexpected scan clauses for UPPER/JOIN RELs")
		}

		// get remote/local exprs from the passed fpinfo rather
		// But for pushed_down JOIN expr there shouldn't be any local_conds (apart from Having clause)

		// fdwRecheckQuals will be empty in case of JOIN rel as it should be added in GetForeignJoinPaths
		// EPQ recheck for upper_rel isn't required, because `SELECT FOR UPDATE.. ` is not allowed

		// for UPPER_REL we've target_list in `grouped_tlist` from getForeignUpperPaths
		targetList = fpinfo.grouped_tlist
		fdwScanTlist = targetList

		ctx := &deparseCtx{
			root:    root,
			scanrel: scanRel,
			buf:     &strings.Builder{},
		}

		// extract column names from targetList into retrievedAttrs and ctx.buf
		deparseExplicitTargetList(targetList, &retrievedAttrs, ctx)

		// put column names from ctx.buf into sqlData
		// we have target columns to be fetched in sqlData.data
		C.initStringInfo(&sqlData)
		C.appendStringInfoString(&sqlData, C.CString(ctx.buf.String()))

		// for UPPERREL and other kinds
		// we've inner REL in `outerrel` which have it's own FdwState
		// and that state have remote expr info
		ifpinfo := (*C.ChFdwRelationInfo)(unsafe.Pointer(fpinfo.outerrel.fdw_private))

		// get the already computed remote Exprs from inner rel
		// these will form the conditions
		remoteExprs = ifpinfo.remote_conds

		// extract local exprs that we may get from getForeignUpperPaths
		localExprs = C.extract_actual_clauses(fpinfo.local_conds, false)

		dctx := &deparseCtx{
			root:    root,
			scanrel: scanRel,
			buf:     &strings.Builder{},
		}
		// append/deparse GROUP BY clause into buffer
		appendGroupByClause(targetList, dctx)

		if dctx.buf.Len() > 0 {
			C.initStringInfo(&groupByData)
			C.appendStringInfoString(&groupByData, C.CString(dctx.buf.String()))
		}

		// this means we have having clause (which we processed in GetForeignUpperPaths)
		if fpinfo.remote_conds != nil {
			hctx := &deparseCtx{
				root:    root,
				scanrel: scanRel,
				buf:     &strings.Builder{},
			}
			appendHaving(fpinfo.remote_conds, hctx)
			if hctx.buf.Len() > 0 {
				C.initStringInfo(&havingData)
				C.appendStringInfoString(&havingData, C.CString(hctx.buf.String()))
			}
		}

		// no support for outer_plan as NO JOINs
		if outer_plan != nil {
			errLogger.Println("unexpected outerplan")
		}

	} else {
		// as currently we only do UPPER_REL only
		errLogger.Println("no support FOR JOINs")
	}
	// we should embed the formatted query clause into the fdw_private

	// table name passed in options has priority
	// make sure no alias is passed with relation
	C.extract_tablename(root, scanRel, &tableData)

	ctx := &deparseCtx{
		root:    root,
		scanrel: scanRel,
		buf:     &strings.Builder{},
	}
	appendConditions(remoteExprs, ctx)
	C.initStringInfo(&conditionsData)
	C.appendStringInfoString(&conditionsData, C.CString(ctx.buf.String()))
	// create a pg list containing the targetColumns, tableNames, conditions and list of attr numbers for target columns
	// and pass it to next stages via fdw_private
	fdwprivate = (*C.List)(unsafe.Pointer(C.wrapper_list_make4(
		C.makeString(sqlData.data), C.makeString(tableData.data), C.makeString(conditionsData.data), retrievedAttrs)))

	//append GROUP BY if any
	fdwprivate = C.wrapper_lappend(fdwprivate, unsafe.Pointer(C.makeString(groupByData.data)))
	// append HAVING
	fdwprivate = C.wrapper_lappend(fdwprivate, unsafe.Pointer(C.makeString(havingData.data)))
	// append base_relIDs which are used in the next stage to extract table names
	fdwprivate = C.wrapper_lappend(fdwprivate, unsafe.Pointer(C.makeInteger(C.bms_next_member(root.all_baserels, -1))))

	if fdwprivate == nil {
		errLogger.Println("unable to create fdw_private list while creating foreign plan")
	}
	// by the end of this function, ideally we should process
	// all push down clauses/optimizations/group bys and remote exprs
	// See `plannodes.h` on structure of ForeignScan node
	// no fdw_scan_tlist yet (no join support)
	// no params_list yet (no param support)

	// passing only those exprs that we consider should be run on local server in `localExprs`
	return C.make_foreignscan(tlist, localExprs, scanRelID, nil, fdwprivate, fdwScanTlist, fdwRecheckQuals, outer_plan)
}

//export chExplainForeignPlan
func chExplainForeignPlan(node *C.ForeignScanState, es *C.ExplainState) {

	var (
		chestate *C.ChFdwExecutionState = (*C.ChFdwExecutionState)(unsafe.Pointer(node.fdw_state))
		estate   *fdwExecState
	)

	// get the go-side exec state to get CH connection and row iterators
	estate = getState(uint64(chestate.tok))

	// put the query formed by the structure we've in the estate
	C.ExplainPropertyText(C.CString("Clickhouse Query"), C.CString(estate.query.String()), es)
}

//export chBeginForeignScan
func chBeginForeignScan(node *C.ForeignScanState, eflags C.int) {

	var (
		chestate *C.ChFdwExecutionState
		fsplan   *C.ForeignScan
		estate   *fdwExecState
		rtindex  C.Index
		rte      *C.RangeTblEntry
		esstate  *C.EState
	)

	// infoLogger.Println("in chBeginForeignScan")
	esstate = node.ss.ps.state
	fsplan = (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))

	if fsplan.scan.scanrelid > 0 {
		rtindex = fsplan.scan.scanrelid
	} else {
		// in case of JOIN or aggregate
		rtindex = C.uint(C.wrapper_intVal((*C.Value)(unsafe.Pointer(C.list_nth(fsplan.fdw_private, 6)))))
	}
	rte = C.wrapper_rt_fetch(rtindex, esstate.es_range_table)

	// foreigntableid := node.ss.ss_currentRelation.rd_id
	foreigntableid := rte.relid
	fTable := C.GetForeignTable(foreigntableid)
	serverOpts := getFServerOptions(Oid(fTable.serverid))

	// currently we're connecting to clickhouse every time a foreign scan
	// starts, maybe we should look into storing a map of CH connections
	ch := parseServerOptions(serverOpts)
	ch.connect()
	// this needs to exported via glue code
	// if eflags & C.EXEC_PLAN_EXPLAIN_ONLY {
	// 	return
	// }

	// get information from the passed fdw_private
	// this should ideally contain the SQL query clause(final)
	// but currently it's simply for BASEREL, columns and relation name
	columns := (*C.Value)(unsafe.Pointer(C.wrapper_list_nth(fsplan.fdw_private, C.int(0))))
	columnsStr := (*C.char)(unsafe.Pointer(C.wrapper_strVal(columns)))
	chTable := (*C.Value)(unsafe.Pointer(C.wrapper_list_nth(fsplan.fdw_private, C.int(1))))
	chTableStr := (*C.char)(unsafe.Pointer(C.wrapper_strVal(chTable)))

	conditions := (*C.Value)(unsafe.Pointer(C.wrapper_list_nth(fsplan.fdw_private, C.int(2))))
	condStr := (*C.char)(unsafe.Pointer(C.wrapper_strVal(conditions)))
	parsedConds := strings.TrimSpace(C.GoString(condStr))

	groupby := (*C.Value)(unsafe.Pointer(C.wrapper_list_nth(fsplan.fdw_private, C.int(4))))
	groupbyStr := (*C.char)(unsafe.Pointer(C.wrapper_strVal(groupby)))
	parsedGroupby := strings.TrimSpace(C.GoString(groupbyStr))

	having := (*C.Value)(unsafe.Pointer(C.wrapper_list_nth(fsplan.fdw_private, C.int(5))))
	havingStr := (*C.char)(unsafe.Pointer(C.wrapper_strVal(having)))
	parsedHaving := strings.TrimSpace(C.GoString(havingStr))

	// parse target columns
	// create a query structure
	// and query CH
	// `sql.Rows` iterator is saved in execState(on Go side)
	// and is used in IteratorForeignScan
	targetColumns := strings.Split(strings.TrimSpace(C.GoString(columnsStr)), ",")
	query := &Query{
		SelectFields: targetColumns,
		From:         strings.TrimSpace(C.GoString(chTableStr)),
	}

	infoLogger.Printf("passed columns %v", targetColumns)
	infoLogger.Printf("passed table %v", query.From)

	//ensure we're not passing "" to Conditions array, empty strings are also counted in length of arrays
	if len(parsedConds) > 0 {
		query.Conditions = []string{parsedConds}
		infoLogger.Printf("passed conditions %v", query.Conditions)
	}

	// add group by
	if len(parsedGroupby) > 0 {
		query.GroupBys = []string{parsedGroupby}
		infoLogger.Printf("passed group by %v", query.GroupBys)
	}
	if len(parsedHaving) > 0 {
		query.Having = []string{parsedHaving}
		infoLogger.Printf("passed having %v", parsedHaving)
	}
	rows, err := ch.query(query.String())
	if err != nil {
		errLogger.Printf("error while querying CH: beginForeignScan %v", err)
		rows = nil
	}

	// note that we maintain a go-side exec state (which has CH connections and row iterators)
	// and c-side exec state which has list of target attributes
	// and an integer key to access the go-side state via map
	estate = &fdwExecState{
		ch:    ch,
		query: query,
		rows:  rows,
	}
	// get the go-side exec state in the map and take the key
	index := saveState(estate)
	chestate = (*C.ChFdwExecutionState)(unsafe.Pointer(C.palloc0(C.sizeof_struct_ChFdwExecutionState)))
	// save the key inside the C-side exec state
	chestate.tok = C.uint64(index)
	// save list of retreived_attrs in c-side exec state
	// this is used in next stage: iterateForeignScan
	chestate.retrieved_attrs = (*C.List)(unsafe.Pointer(C.wrapper_list_nth(fsplan.fdw_private, C.int(3))))
	node.fdw_state = unsafe.Pointer(chestate)
}

//export chIterateForeginScan
func chIterateForeginScan(node *C.ForeignScanState) *C.TupleTableSlot {
	var (
		slot            *C.TupleTableSlot      = node.ss.ss_ScanTupleSlot
		tupleDescriptor C.TupleDesc            = slot.tts_tupleDescriptor
		chestate        *C.ChFdwExecutionState = (*C.ChFdwExecutionState)(unsafe.Pointer(node.fdw_state))
		estate          *fdwExecState
		rows            *sql.Rows
		chcolumnindex   int

		crow  *C.Datum = slot.tts_values
		cnull *C.bool  = slot.tts_isnull
	)

	// infoLogger.Println("in chIterateForeginScan")
	C.ExecClearTuple(slot)

	// get the go-side exec state to get CH connection and row iterators
	estate = getState(uint64(chestate.tok))
	rows = estate.rows
	// we can't found any *sql.Rows
	// this shouldn't happen
	if rows == nil {
		errLogger.Println("rows state been found nil :IterateForeignScan")
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		errLogger.Printf("error while fetching columnTypes from CH: IterateForeignScan %v", err)
	}

	if len(columnTypes) != int(C.list_length(chestate.retrieved_attrs)) {
		errLogger.Printf("mismatched number of columns fetched from CH vs number of target columns in PG")
	}

	ok := rows.Next()
	// if rows are finished, return
	if !ok {

		if err := rows.Err(); err != nil {
			errLogger.Printf("error while fetching rows from CH: IterateForeignScan %v", err)
		}
		return slot
	}

	// now we should scan the row and convert CH types -> PG types based on fetched columns
	columns := make([]interface{}, len(columnTypes))
	columnPointers := make([]interface{}, len(columnTypes))

	for i := 0; i < len(columnTypes); i++ {
		columnPointers[i] = &columns[i]
	}

	if err := rows.Scan(columnPointers...); err != nil {
		errLogger.Printf("error while scanning rows from CH: IterateForeignScan %v", err)
		return slot
	}

	// init C Datum row which will hold the data with 0
	C.memset(unsafe.Pointer(crow), C.int(0), C.ulong(C.sizeof_Datum*tupleDescriptor.natts))
	// by default set all attr having null value
	C.memset(unsafe.Pointer(cnull), C.int(1), C.ulong(C.sizeof_bool*tupleDescriptor.natts))

	//We have attr numbers(ref to columns) of PG tables in a list
	//We need to get the type for these PG columns and
	//convert the corresponding CH table column into that PG type
	//BIG NOTE: we assume that the order of the attrs are in this list
	//is in the same order as we've scanned from CH
	//only then the PG type conversion will work
	for cell := C.list_head(chestate.retrieved_attrs); cell != nil; cell = C.wrapper_lnext(chestate.retrieved_attrs, cell) {

		// get the attribute number from list
		// list/l functions are macros in PG and we have defined wrappers for them to use them here
		// attnum are indexes in the list that starts from 1, we adjust them to denote actual values in rowItem array
		attnum := C.wrapper_lfirst_int(cell) - C.int(1)

		// we are accessing attribute array of tupleDescriptor(see include/access/tupdesc.h for ref)
		// which is of type FormData_pg_attribute (see catalog/pg_attribute.h for ref)

		// from PG docs:
		// atttypid is the OID of the instance in Catalog Class pg_type that
		// defines the data type of this attribute (e.g. int4).  Information in
		// that instance is redundant with the attlen, attbyval, and attalign
		// attributes of this instance.
		pgtype := (*C.FormData_pg_attribute)(unsafe.Pointer(C.wrapper_TupleDescAttr(tupleDescriptor, attnum))).atttypid

		//from PG docs:
		// atttypmod records type-specific data supplied at table creation time
		// (for example, the max length of a varchar field).  It is passed to
		// type-specific input and output functions as the third argument. The
		// value will generally be -1 for types that do not need typmod.
		pgtypemod := (*C.FormData_pg_attribute)(unsafe.Pointer(C.wrapper_TupleDescAttr(tupleDescriptor, attnum))).atttypmod

		//if the column fetched is null move ahead
		//by default, we mark all columns as null
		if columns[chcolumnindex] == nil {
			continue
		}
		//if not nil mark this column as not null
		//somewhat convoluted pointer arinthmetic
		//in short, we are trying to access `attnum` index in the pointer array
		item := (*C.bool)(unsafe.Pointer(uintptr(unsafe.Pointer(cnull)) + uintptr(attnum)*uintptr(C.sizeof_bool)))
		*item = C.bool(false)

		// (this doesn't work for fetching rowItem, segfaults)
		// item := (*C.bool)(unsafe.Pointer(C.get_ptr(unsafe.Pointer(cnull), attnum)))
		// *item = C.bool(false)

		//in short: we are trying to access `attnum` index in the pointer array
		rowItem := (*C.Datum)(unsafe.Pointer(uintptr(unsafe.Pointer(crow)) + uintptr(attnum)*uintptr(C.sizeof_Datum)))
		*rowItem = getDatumForChType(pgtype, pgtypemod, columns[chcolumnindex], columnTypes[chcolumnindex])
		chcolumnindex++
	}
	C.ExecStoreVirtualTuple(slot)
	return slot

}

//export chReScanForeignScan
func chReScanForeignScan(node *C.ForeignScanState) {

	var (
		chestate *C.ChFdwExecutionState = (*C.ChFdwExecutionState)(unsafe.Pointer(node.fdw_state))
		estate   *fdwExecState
		rows     *sql.Rows
	)
	estate = getState(uint64(chestate.tok))
	// on rescan
	// reinit the query rows
	estate.rows.Close()

	rows, err := estate.ch.query(estate.query.String())
	if err != nil {
		errLogger.Printf("error while querying CH: reScanForeignScan %v", err)
		estate.rows = nil
	}
	estate.rows = rows

}

//export chEndForeignScan
func chEndForeignScan(node *C.ForeignScanState) {

	var (
		estate   *fdwExecState
		rows     *sql.Rows
		chestate *C.ChFdwExecutionState = (*C.ChFdwExecutionState)(unsafe.Pointer(node.fdw_state))
	)

	estate = getState(uint64(chestate.tok))
	rows = estate.rows
	if rows != nil {
		rows.Close()
		estate.rows = nil
	}
	deleteState(uint64(chestate.tok))
}

//export chAnalyzeForeignTable
func chAnalyzeForeignTable(relation C.Relation, AquireSampleRowsFunc *C.AcquireSampleRowsFunc, totalpages *C.BlockNumber) C.bool {

	*totalpages = 1
	return true
}

// convert value to Datum based on columnType from CH and PG expected type
// this is heavily inspired from https://github.com/pgspider/sqlite_fdw
// could have been easier done in C as we reference so many macro functions but can't use directly in GO without wrappers
// we chose to do this because we recv the CH values as Go types and passing Go datatypes back to C land will much more difficult
func getDatumForChType(pgtype C.Oid, pgtypemod C.int, value interface{}, columnType *sql.ColumnType) C.Datum {
	var (
		valueDatum C.Datum
		final      C.Datum
		//type input function
		typeinput C.regproc
		//type modifier
		typemod C.int
		tuple   C.HeapTuple
	)
	// Idea is that every type in PG has a type-input function: see https://www.postgresql.org/docs/10/xtypes.html for ref
	// we go to these extents in case of types like
	// variable size strings varchar(20), varchar(30) and any other types that we don't anticipate
	// so we get the type's input function and call it with `valueDatum`
	// OidFunctionCall3 is used for the calling the function with provided args

	/* get the type's output function from system cache*/
	tuple = C.wrapper_SearchSysCache1Oid(C.wrapper_ObjectIdGetDatum(pgtype))
	if bool(!C.wrapper_HeapTupleIsValid(tuple)) {
		errLogger.Printf("cache lookup failed for type %v", pgtype)
	}

	// we convert the HeapTuple pointer to pg_type struct
	// and access the input function aand modifier
	// see include/catalog/pg_type.h for details on these fields
	typeinput = ((C.Form_pg_type)(unsafe.Pointer(C.wrapper_GETSTRUCT(tuple)))).typinput
	typemod = ((C.Form_pg_type)(unsafe.Pointer(C.wrapper_GETSTRUCT(tuple)))).typtypmod
	C.ReleaseSysCache(tuple)

	// switch on the passed types's OID and match the same with passed CH type
	// Note that we do the type conversions on best-effort basis here.
	// if there's a type that doesn't get handled by default case
	// as in its string-repr isn't a valid expr on PG side
	// that will come up as error
	// but we should fix them retroactively
	switch pgtype {

	// we plan to map every CH type to NUMERIC type in PG
	// so if the target column is of numeric type
	// do conversion based on underlying CH type
	// note that we don't have a direct wrapper_NumericGetDatum(__)
	// because it won't work directly as there is a external conversion required as well.
	// so we convert the input value to string repr, and rely on numeric's input function `numeric_in` to convert it to Numeric DataType
	// for details see https://stackoverflow.com/questions/48448064/postgresql-c-how-to-convert-uint64-t-into-numeric
	// the numeric type is coded at utils/adt/numeric.c
	case C.NUMERICOID:
		// infoLogger.Printf("found numeric vs column type %v", columnType.DatabaseTypeName())
		if columnType.DatabaseTypeName() == "UInt64" {
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value.(uint64))))
		} else if columnType.DatabaseTypeName() == "UInt32" {
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value.(uint32))))
		} else if columnType.DatabaseTypeName() == "UInt16" {
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value.(uint16))))
		} else if columnType.DatabaseTypeName() == "UInt8" {
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value.(uint8))))
		} else if columnType.DatabaseTypeName() == "Int64" {
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value.(int64))))
		} else if columnType.DatabaseTypeName() == "Int32" {
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value.(int32))))
		} else if columnType.DatabaseTypeName() == "Int16" {
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value.(int16))))
		} else if columnType.DatabaseTypeName() == "Int8" {
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value.(int8))))
		} else {
			// this should catch everything, but we're mentioning explicit type assertions above so a type still can fallthrough
			// this should also capture decimal values from CH column
			// given the target column is defined either with `numeric` or `numeric(precision, scale)`
			// see https://www.postgresql.org/docs/11/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
			// specifying just `numeric` means values of any precision and scale can be stored
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value)))
		}
		// now we hope that auto type conversion by typeinput will convert valueDatum to Numeric datum

	// for date type, we follow the same strategy of relying on the type's input function
	// see utils/adt/date.c for `date_in`
	// parse the available CH value in format `YYYY-MM-DD` and pass it to PG type's input function
	// we map CH's `Date` type to PG `date` type
	case C.DATEOID:
		parsedDate := value.(time.Time)
		valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", parsedDate.Format("2006-01-02"))))

	// for timestamptz type, we rely on type's input function
	// see utils/adt/timestamp.c for input function `timestamptz_in`
	// parse the available CH value in format `2006-01-02T15:04:05Z07:00` (RFC3339) and pass it to PG type's input function
	// we convert CH's `DateTime` type to PG `timestamptz` type

	// Note: CH `DateTime` type always returns values in UTC timezone
	// although it's possible to annotate `DateTime` with another tz like `DateTime(CET)`
	// the returned values from the CH golang driver doesn't returned that tz info.
	// so we convert the same into PG tz type
	// If the returned value holds the tz info, that will be parsed accordingly.
	// displayed tz on `psql` client uses the client's timezone and converts to that.
	case C.TIMESTAMPTZOID:
		parsedDateTime := value.(time.Time)
		// debugLogger.Println("ch tz type", columnType.DatabaseTypeName())
		// debugLogger.Println("found timestamp with value %v", parsedDateTime.Format(time.RFC3339))
		valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", parsedDateTime.Format(time.RFC3339))))

	case C.FLOAT4OID:
		typeV := reflect.TypeOf(value)
		switch typeV.String() {
		case "float32":
			return C.wrapper_Float4GetDatum(C.float(value.(float32)))
		default:
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value)))
		}
	case C.FLOAT8OID:
		typeV := reflect.TypeOf(value)
		switch typeV.String() {
		case "float64":
			return C.wrapper_Float8GetDatum(C.double(value.(float64)))
		case "float32":
			return C.wrapper_Float8GetDatum(C.double(value.(float32)))
		default:
			valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value)))
		}
	case C.INT4OID:
		if columnType.DatabaseTypeName() == "UInt32" {
			return C.wrapper_Int32GetDatum(C.int32((value.(uint32))))
		} else if columnType.DatabaseTypeName() == "Int32" {
			return C.wrapper_Int32GetDatum(C.int32(value.(int32)))
		} else {
			errLogger.Printf("no appropriate conversion found for %v -> %v", columnType.DatabaseTypeName(), "INT4")
		}
	case C.INT8OID:
		if columnType.DatabaseTypeName() == "UInt64" {
			return C.wrapper_Int64GetDatum(C.int64((value.(uint64))))
		} else if columnType.DatabaseTypeName() == "Int64" {
			return C.wrapper_Int64GetDatum(C.int64(value.(int64)))
		} else {
			errLogger.Printf("no appropriate conversion found for %v -> %v", columnType.DatabaseTypeName(), "INT8")
		}
	case C.INT4ARRAYOID:
		fallthrough
	case C.INT8ARRAYOID:
		fallthrough
	case C.OIDARRAYOID:
		fallthrough
	case C.FLOAT4ARRAYOID:
		fallthrough
	case C.FLOAT8ARRAYOID:
		fallthrough
	case C.NUMERICARRAYOID:
		parsedArray := fmt.Sprintf("%v", value)
		parsedArray = strings.Replace(parsedArray, "[", "{", -1)
		parsedArray = strings.Replace(parsedArray, "]", "}", -1)
		// join array elements with ","
		parsedArray = strings.Join(strings.Split(parsedArray, " "), ",")
		// parsedArray = fmt.Sprintf("'%s'", parsedArray)
		valueDatum = C.wrapper_CStringGetDatum(C.CString(parsedArray))
		// get Oid for element type of the array
		// see pg_type.h for reference
		typeelem := ((C.Form_pg_type)(unsafe.Pointer(C.wrapper_GETSTRUCT(tuple)))).typelem
		// infoLogger.Println(" casting ... for array", parsedArray, int(typeinput), int(typeelem), int(typemod))
		// pass typeelem as arg to `array_in` function
		final = C.wrapper_OidFunctionCall3(typeinput, valueDatum, C.wrapper_ObjectIdGetDatum(typeelem), C.wrapper_Int32GetDatum(typemod))
		return final

	case C.TEXTARRAYOID:
		fallthrough
	case C.VARCHARARRAYOID:

		// Note: We need to find out the underlying array data type is FixedString(X) or String
		// because null-terminated FixedString() doesn't work well with `%v`
		if strings.HasPrefix(columnType.DatabaseTypeName(), "Array(FixedString") {
			errLogger.Println("FixedString array support on the way")

		} else if strings.HasPrefix(columnType.DatabaseTypeName(), "Array(String") {

		} else {
			errLogger.Println("unrecoginzed remote array type from CH")
		}
		assertedValue := value.([]string)
		buf := &strings.Builder{}
		buf.WriteString("{")
		for i, item := range assertedValue {
			item = fmt.Sprintf("'%s'", item)
			if i != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(item)
		}
		buf.WriteString("}")
		// infoLogger.Println(" casting ... for array", buf.String())

		valueDatum = C.wrapper_CStringGetDatum(C.CString(buf.String()))
		// get Oid for element type of the array
		// see pg_type.h for reference
		typeelem := ((C.Form_pg_type)(unsafe.Pointer(C.wrapper_GETSTRUCT(tuple)))).typelem
		// infoLogger.Println(" casting ... for array", parsedArray, int(typeinput), int(typeelem), int(typemod))
		// pass typeelem as arg to `array_in` function
		final = C.wrapper_OidFunctionCall3(typeinput, valueDatum, C.wrapper_ObjectIdGetDatum(typeelem), C.wrapper_Int32GetDatum(typemod))
		return final
	default:
		// by default cast to string
		valueDatum = C.wrapper_CStringGetDatum(C.CString(fmt.Sprintf("%v", value)))
	}

	final = C.wrapper_OidFunctionCall3(typeinput, valueDatum, C.wrapper_ObjectIdGetDatum(C.InvalidOid), C.wrapper_Int32GetDatum(typemod))
	return final
}

// GetForeignUpperPaths adds a path for post-join/post-scan operations like aggregation and grouping.
// We first evaluate the expr/operations if they can be pushed down or not or if the particular stage is one that we don't support.
// Input rel is the actual input relation involved in the aggregation/join/grouping. Note that `fdw_private` of input_rel should have
// all the information that we've already generated (regarding path, target columns and restriction clause) as this step is called after GetForeignRelSize.
// We can choose to not return any path, in case the operation is not possible, in that case PG will apply the operation locally.
// We extract target_list from the `PlannerInfo` and evaluate them to be passed to foreign server.
// All useful information regarding grouping target list, startup costs etc are put in `fdw_private` of the output_rel
// We use the same struct in downstream stages detecting that the passed rel is UPPER_RELOPT (upper relation that is).
// Next flow of control should be invoked by chGetForeignPlan -> chBeginForeignScan -> chIterateForeignScan and so forth
// Currently we only support 'UPPERREL_GROUP_AGG' as the only stage (UpperRelationKind) (see relation.h for reference)
// This function heavily takes ideas from sqlite_fdw's `GetForeignUpperPaths`
//export chGetForeignUpperPaths
func chGetForeignUpperPaths(root *C.PlannerInfo, stage C.UpperRelationKind, input_rel *C.RelOptInfo, output_rel *C.RelOptInfo, extra unsafe.Pointer) {

	var (
		// create new fdwRelation state to attach to output_rel
		fpinfo *C.ChFdwRelationInfo
		// input fdwrelation state
		ifpinfo            *C.ChFdwRelationInfo
		parsedQuery        *C.Query
		groupingTargets    *C.PathTarget
		actualGroupTargets *C.PathTarget
		targetList         *C.List
		aggVars            *C.List
		groupPath          *C.ForeignPath
		rows               C.double
		startupCost        C.Cost
		totalCost          C.Cost
		pathkeys           *C.List
		required_outer     *C.Bitmapset // no required outer
		fdw_outerpath      *C.Path      // no outer path
		no_private         *C.List      // no fdw_private that is externally passed
		//width can be used if we reduce default width of rows

	)

	// infoLogger.Println("in chGetForeignUpperPaths")
	// if input_rel is somehow uninitialized, return
	if input_rel.fdw_private == nil {
		return
	}

	// only supporting result of grouping/aggregation at the time
	// so ignore everything else
	if stage != C.UPPERREL_GROUP_AGG {
		return
	}

	// ignore duplicate calls
	if output_rel.fdw_private != nil {
		return
	}

	// attach state to output_rel
	fpinfo = (*C.ChFdwRelationInfo)(unsafe.Pointer(C.palloc0(C.sizeof_struct_ChFdwRelationInfo)))
	output_rel.fdw_private = unsafe.Pointer(fpinfo)

	//mark current input_rel as outerrel for the output_rel fdw state
	fpinfo.outerrel = input_rel
	parsedQuery = (*C.Query)(unsafe.Pointer(root.parse))

	// no support for grouping sets yet
	if parsedQuery.groupingSets != nil {
		return
	}
	// see parsenodes.h for details on Query struct
	// return early if no aggregation/groupsets or havings are not present
	// This should be modified accordingly as more support is added.
	if parsedQuery.groupClause == nil && parsedQuery.hasAggs == C.bool(false) && root.hasHavingQual == C.bool(false) {
		return
	}

	ifpinfo = (*C.ChFdwRelationInfo)(unsafe.Pointer(input_rel.fdw_private))
	// if passed input_rel has local conditions (which needs to be executed on PG server) they should be evaluated
	// before applying aggregations on remote, so it doesn't make sense to push down aggregation
	if ifpinfo.local_conds != nil {
		return
	}

	// get list of PathTargets from root
	groupingTargets = root.upper_targets[stage]

	// copy group targets (for reasons to be explained...)
	actualGroupTargets = C.copy_pathtarget(groupingTargets)

	// check if groupingTargets are eligible for exectution on CH server (see relation.h for details on PathTarget struct)
	// and also extract out target_list onto fpinfo we created to be pushed downstream
	// Every expr in `actualGroupTargets` which is part of GROUP BY will need to pushed down
	// and we don't make a distinction of local/remote group by in this case
	index := 0
	for cell := C.list_head(actualGroupTargets.exprs); cell != nil; cell = C.wrapper_lnext(actualGroupTargets.exprs, cell) {
		expr := (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(cell)))
		// use a macro to get actual group refno from `sortgroupref` array
		groupingref := C.wrapper_get_pathtarget_sortgroupref(actualGroupTargets, C.int(index))

		// if expr topics is part of GROUP BY clause
		if groupingref != 0 && C.get_sortgroupref_clause_noerr(groupingref, parsedQuery.groupClause) != nil {
			// check if the GROUP BY expr is valid on CH server
			if !C.is_foreign_expr(root, output_rel, expr) {
				// if not continue on other group by targets
				// there can be other exprs which are eligible for pushing
				// return
				// infoLogger.Println("found false group by getUpperPath")
				goto label
			}

			targetList = (*C.List)(unsafe.Pointer(
				C.add_to_flat_tlist(
					targetList, (*C.List)(unsafe.Pointer(C.wrapper_list_make1(unsafe.Pointer(expr)))),
				)),
			)

		} else {
			// this means that either we don't have a GROUP BY clause or we don't have a SortGroupClause at all

			// first check if the expr is eligible for execution on remote server in its whole
			// this involves checking for existing ORDER BYs as well
			if C.is_foreign_expr(root, output_rel, expr) {

				// add directly to the targetlist
				targetList = (*C.List)(unsafe.Pointer(
					C.add_to_flat_tlist(
						targetList, (*C.List)(unsafe.Pointer(C.wrapper_list_make1(unsafe.Pointer(expr)))),
					)),
				)
			} else {
				// so now either a ORDER BY clause is there or aggregate Var exprs

				// if there's a valid SortGroupClause means we've ORDER BY clause
				if groupingref != 0 {
					// no support for ORDER BY yet, clear refnos thus
					// actualGroupTargets.sortgrouprefs[index] = C.int(0)
					// do pointer arithmetic because of course go won't allow [] on *Pointers
					item := (*C.uint)(unsafe.Pointer(uintptr(unsafe.Pointer(actualGroupTargets.sortgrouprefs)) + uintptr(index)*uintptr(C.sizeof_uint)))
					*item = C.uint(0)
				}

				// no ORDER BY clause but might be aggregates Vars
				// see optimizer/var.h for references
				// we pull aggregate Vars from the given expr into aggVars
				aggVars = C.pull_var_clause(
					(*C.Node)(unsafe.Pointer(expr)),
					C.PVC_INCLUDE_AGGREGATES,
				)

				// if pulled aggVars are eligible for remote execution
				if !C.is_foreign_expr(root, output_rel, (*C.Expr)(unsafe.Pointer(aggVars))) {
					// if not eligible continue on other exprs to check them as well
					goto label
				}

				// now make sure that pulled Vars(columns) are of type `Aggref` and not plain columns
				// Plain Vars should be added above. (either a part of GROUP BY expr or the whole GROUP BY expr)
				for l := C.list_head(aggVars); l != nil; l = C.wrapper_lnext(aggVars, l) {
					expr = (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(l)))

					if C.wrapper_nodeTag((*C.Node)(unsafe.Pointer(expr))) == C.T_Aggref {
						targetList = (*C.List)(unsafe.Pointer(
							C.add_to_flat_tlist(
								targetList, (*C.List)(unsafe.Pointer(C.wrapper_list_make1(unsafe.Pointer(expr)))),
							)),
						)
					}
				}
			}

		}
	label:
		index++

	}

	if C.list_length(targetList) == 0 {
		// no appropriate targetList
		return
	}

	if root.hasHavingQual && parsedQuery.havingQual != nil {

		havingQualList := (*C.List)(unsafe.Pointer(parsedQuery.havingQual))
		for cell := C.list_head(havingQualList); cell != nil; cell = C.wrapper_lnext(havingQualList, cell) {
			expr := (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(cell)))

			// we're only interested in RestrictInfo clauses
			// wraper expr into a RestrinctInfo structure
			// see optimizer/restrictinfo.h for fuction ref
			rinfo := C.make_restrictinfo(expr, C.bool(true), C.bool(false), C.bool(false), root.qual_security_level, output_rel.relids, nil, nil)

			// check to see eligibility of having clause expr
			if C.is_foreign_expr(root, output_rel, expr) {
				fpinfo.remote_conds = C.wrapper_lappend(fpinfo.remote_conds, unsafe.Pointer(rinfo))
			} else {
				// If the whole expr is not eligible to be pushed remotely
				// pull Vars and aggregates from it and see if they can be pushed
				aggVars = C.pull_var_clause(
					(*C.Node)(unsafe.Pointer(expr)),
					C.PVC_INCLUDE_AGGREGATES,
				)

				// see if pulled aggVars aggregate functions are eligible for remote execution
				if C.wrapper_nodeTag((*C.Node)(unsafe.Pointer(aggVars))) == C.T_Aggref {

					if C.is_foreign_expr(root, output_rel, (*C.Expr)(unsafe.Pointer(aggVars))) {
						targetList = (*C.List)(unsafe.Pointer(
							C.add_to_flat_tlist(
								targetList, (*C.List)(unsafe.Pointer(C.wrapper_list_make1(unsafe.Pointer(expr)))),
							)),
						)
					} else {
						// if not eligible that means a aggregate within local conditions are not being pushed down
						// although this should not happen but we avoid returning a PATH in that case
						return
					}
				}

				// if no aggref then simply append them to local_conds
				fpinfo.local_conds = C.wrapper_lappend(fpinfo.local_conds, unsafe.Pointer(rinfo))
			}
		}

	}
	// this is needed as we should add matching `tle->ressortgroupref` for the sortgrouprefs that we
	// extracted above in tlist
	C.apply_pathtarget_labeling_to_tlist(targetList, actualGroupTargets)

	// attach the prepared targetList which will be used in the later stage
	fpinfo.grouped_tlist = targetList

	// constant costs for computation (only CPU cycles)
	rows = C.double(1)
	startupCost = C.double(1)
	totalCost = C.double(1)

	groupPath = C.create_foreignscan_path(
		root,
		output_rel,
		groupingTargets,
		rows, startupCost, totalCost, pathkeys, required_outer, fdw_outerpath, no_private)

	C.add_path(output_rel, (*C.Path)(unsafe.Pointer(groupPath)))
}

func main() {
	fmt.Println("Hello")
}
