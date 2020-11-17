package main

/*
#cgo darwin CFLAGS: -I/usr/local/Cellar/postgresql/11.1/include/internal -I/usr/local/Cellar/postgresql/11.1/include/server
#cgo linux CFLAGS: -I/usr/include/postgresql/11/server -I/usr/include/postgresql/internal
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "ch_helpers.h"
*/
import "C"

import (
	"fmt"
	"strings"
	"unsafe"
)

type deparseCtx struct {

	//global planner state
	root *C.PlannerInfo
	// the foreign relation we are planning for
	// foreignrel *C.RelOptInfo
	// the underlying scan relation. Same as
	// foreignrel, when that represents a join or
	// a base relation.
	scanrel *C.RelOptInfo
	// output buffer
	buf *strings.Builder
	// exprs that will become remote Params
	// not supported yet
	// List	  **params_list;
}

// if args order changes btwn functions
// deparse function should take care of that
var mapPGfuncToCH = map[string]string{
	"lower":     "lower",
	"upper":     "upper",
	"replace":   "replaceAll",
	"abs":       "abs",
	"round":     "round",
	"substr":    "substring",
	"sum":       "sum",
	"avg":       "avg",
	"max":       "max",
	"min":       "min",
	"count":     "count",
	"date_part": "__placeholder__",
	"timestamp": "toDateTime",
}

var tsDurations = []string{"'month'", "'year'", "'minute'", "'second'", "'hour'"}

func appendConditions(exprs *C.List, ctx *deparseCtx) {

	var expr *C.Expr
	var first = true
	buf := ctx.buf
	for cell := C.list_head(exprs); cell != nil; cell = C.wrapper_lnext(exprs, cell) {

		item := (unsafe.Pointer(C.wrapper_lfirst(cell)))
		// if cell is of type RestrictInfo, get expr from its `clause`
		if C.wrapper_nodeTag((*C.Node)(item)) == C.T_RestrictInfo {
			ri := (*C.RestrictInfo)(item)
			expr = (*C.Expr)(unsafe.Pointer(ri.clause))
		} else {
			expr = (*C.Expr)(item)
		}

		if !first {
			buf.WriteString(" AND ")
		}

		buf.WriteString("(")
		deparseExpr(expr, ctx)
		buf.WriteString(")")
		first = false
	}

}

// deparseExpr receives different expr types
// and build the string buffer (which gets passed to CH)
// see primenodes.h for references on these types
func deparseExpr(expr *C.Expr, ctx *deparseCtx) {

	node := (*C.Node)(unsafe.Pointer(expr))
	if node == nil {
		return
	}

	switch C.wrapper_nodeTag(node) {
	case C.T_Const:
		deparseConst((*C.Const)(unsafe.Pointer(node)), ctx)
	case C.T_Var:
		deparseVar((*C.Var)(unsafe.Pointer(node)), ctx)
	case C.T_OpExpr:
		deparseOpExpr((*C.OpExpr)(unsafe.Pointer(node)), ctx)
	case C.T_FuncExpr:
		deparseFuncExpr((*C.FuncExpr)(unsafe.Pointer(node)), ctx)
	case C.T_ScalarArrayOpExpr:
		deparseScalarArrayOpExpr((*C.ScalarArrayOpExpr)(unsafe.Pointer(node)), ctx)
	case C.T_RelabelType:
		deparseRelabelType((*C.RelabelType)(unsafe.Pointer(node)), ctx)
	case C.T_BoolExpr:
		deparseBoolExpr((*C.BoolExpr)(unsafe.Pointer(node)), ctx)
	case C.T_NullTest:
		deparseNullTest((*C.NullTest)(unsafe.Pointer(node)), ctx)
	case C.T_ArrayExpr:
		deparseArrayExpr((*C.ArrayExpr)(unsafe.Pointer(node)), ctx)
	case C.T_CaseExpr:
		deparseCaseExpr((*C.CaseExpr)(unsafe.Pointer(node)), ctx)
	case C.T_CoalesceExpr:
		deparseCoalesceExpr((*C.CoalesceExpr)(unsafe.Pointer(node)), ctx)
	case C.T_NullIfExpr:
		deparseNullIfExpr((*C.NullIfExpr)(unsafe.Pointer(node)), ctx)
	case C.T_Aggref:
		deparseAggref((*C.Aggref)(unsafe.Pointer(node)), ctx)
	default:
		errLogger.Printf("unsupported expression type %v to deparse", int(C.wrapper_nodeTag(node)))
	}
}

// deparseOpExpr: for an operator invocation
// `=`, `>`, `<`, `<>` and so forth
// mapping to CH function is done on best effort basis
// we try to report operators that are not available on CH
// more operator exceptions can be added
func deparseOpExpr(node *C.OpExpr, ctx *deparseCtx) {
	var (
		tuple   C.HeapTuple
		form    C.Form_pg_operator
		oprkind C.char
		opname  string
		arg     *C.ListCell
	)
	buf := ctx.buf

	/* get the type's output function from system cache*/
	tuple = C.wrapper_SearchSysCache1(C.OPEROID, C.wrapper_ObjectIdGetDatum(node.opno))
	if bool(!C.wrapper_HeapTupleIsValid(tuple)) {
		errLogger.Printf("cache lookup failed for operator %v", node.opno)
	}

	// we convert the HeapTuple pointer to pg_form_operator struct
	form = (C.Form_pg_operator)(unsafe.Pointer(C.wrapper_GETSTRUCT(tuple)))
	oprkind = form.oprkind

	// make sure that length of args passed to operator are correct
	opname = C.GoString(&form.oprname.data[0])
	// fmt.Println("oprkind ", oprkind, int(C.list_length(node.args)), " opname ", opname)
	if !((oprkind == C.char('r') && C.list_length(node.args) == C.int(1)) ||
		(oprkind == C.char('l') && C.list_length(node.args) == C.int(1)) ||
		(oprkind == C.char('b') && C.list_length(node.args) == C.int(2))) {
		errLogger.Printf("incorrect args length to operator %v", node.opno)
	}

	buf.WriteString("(")

	// deparse left operand
	if oprkind == C.char('r') || oprkind == C.char('b') {
		arg = C.list_head(node.args)
		deparseExpr((*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(arg))), ctx)
		buf.WriteString(" ")
	}

	// deparse operator name
	// keeping it simple now
	// no overriding or remapping operator name if catalog namespace is different

	opname = C.GoString(&form.oprname.data[0])
	// only considering name PG_CATALOG_NAMESPACE for form.oprnamespace
	if opname == "~~" {
		buf.WriteString("LIKE")
	} else if opname == "!~~" {
		buf.WriteString("NOT LIKE")
	} else if opname == "~~*" || opname == "!~~*" || opname == "~" || opname == "!~" || opname == "~*" || opname == "!~*" {
		errLogger.Printf("operator is not supported")
	} else {
		buf.WriteString(opname)
	}

	//deparse right operand
	if oprkind == C.char('l') || oprkind == C.char('b') {
		arg = C.list_tail(node.args)
		buf.WriteString(" ")
		deparseExpr((*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(arg))), ctx)
	}

	buf.WriteString(")")
	C.ReleaseSysCache(tuple)
}

// deparseVar: node representing a table column
// we already have a deparse_column_ref which is used here
func deparseVar(node *C.Var, ctx *deparseCtx) {
	var (
		column      C.StringInfoData
		relIds      C.Relids
		varlevelsup C.Index
	)
	C.initStringInfo(&column)
	relIds = ctx.scanrel.relids
	varlevelsup = node.varlevelsup
	buf := ctx.buf

	if bool(C.bms_is_member(C.int(node.varno), relIds)) && C.uint(varlevelsup) == C.uint(0) {
		//Var belongs to foreign tabl
		C.deparse_column_ref(&column, C.int(node.varno), C.int(node.varattno), ctx.root)
	} else {
		// no param support
		errLogger.Printf("no params support yet")
	}
	buf.WriteString(strings.TrimSpace((C.GoString(column.data))))
}

// deparseConst for all different constant types
// see get_const_expr() in ruleutils.c for ideas
func deparseConst(node *C.Const, ctx *deparseCtx) {
	var (
		typeoutput   C.Oid
		typeIsVarLen C.bool
		actualValue  *C.char
	)
	buf := ctx.buf

	if bool(node.constisnull) {
		buf.WriteString("NULL")
		return
	}
	// extract type info of the constant into the typeoutput and typeIsVarLen
	C.getTypeOutputInfo(node.consttype, &typeoutput, &typeIsVarLen)

	// Note that we do the type conversions on best-effort basis here.
	// if there's a type that doesn't get handled by default case
	// as in its string-repr isn't a valid expr on CH side
	// that will come up as error
	// but we should fix them retroactively
	switch node.consttype {
	case C.INT2OID:
		fallthrough
	case C.INT4OID:
		fallthrough
	case C.INT8OID:
		fallthrough
	case C.OIDOID:
		fallthrough
	case C.FLOAT4OID:
		fallthrough
	case C.FLOAT8OID:
		fallthrough
	case C.NUMERICOID:
		actualValue = C.OidOutputFunctionCall(typeoutput, node.constvalue)
		// if special values like NaN quote them
		// numbers, decimals with exponents can be passed as such
		// strspn returns the length of the initial portion of str1 which consists only of characters that are part of str2.
		if C.strspn(actualValue, C.CString("0123456789+-eE.")) == C.strlen(actualValue) {
			// no modifications to `-` or `+` signs
			buf.WriteString(C.GoString(actualValue))
		} else {
			// either 'NaN' or 'infinity' ??
			buf.WriteString(fmt.Sprintf("'%s'", C.GoString(actualValue)))
		}
		// handle booleans, binary values, bits
	case C.BITOID:
	case C.VARBITOID:
	case C.BYTEAOID:
		// there is no binary types in CH
		// everything should map to String/FixedString

		// the string for BYTEA always seems to be in the format "\\x##"
		// where # is a hex digit, Even if the value passed in is
		// 'hi'::bytea we will receive "\x6869". Making this assumption
		// allows us to quickly convert postgres escaped strings to sqlite
		// ones for comparison
		// TODO: find appropriate parsing scheme for passing hex values

		errLogger.Println("binary type deparse not supported")
		// actualValue = C.OidOutputFunctionCall(typeoutput, node.constvalue)
		// buf.WriteString(C.GoString(actualValue))

	// scalaryArrayExpr are handeled in parseConstArrays
	// the cases when these blocks are executed are different
	// mostly when we've x = '{1, 2, 3}' or x = ARRAY[1, 2] etc.
	case C.INT2ARRAYOID:
		fallthrough
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
		// extract output values
		// remove leading and trailing '}' and '{'
		actualValue = C.OidOutputFunctionCall(typeoutput, node.constvalue)
		value := C.GoString(actualValue)
		value = strings.TrimLeft(value, "{")
		value = strings.TrimRight(value, "}")
		// we're considering only these numeric arrays
		// so no escaping for them
		// values are already concat with ',' so just print them
		buf.WriteString("[")
		buf.WriteString(value)
		buf.WriteString("]")
	case C.TEXTARRAYOID:
		fallthrough
	case C.VARCHARARRAYOID:
		// extract output values
		// remove leading and trailing '}' and '{'
		actualValue = C.OidOutputFunctionCall(typeoutput, node.constvalue)
		value := C.GoString(actualValue)
		value = strings.TrimLeft(value, "{")
		value = strings.TrimRight(value, "}")
		// quote every item of passed array (after removing'{' and '}')
		// standard lib doesn't provide a way for quoting with single quotes
		separated := strings.Split(value, ",")

		buf.WriteString("[")
		for i, item := range separated {
			item = fmt.Sprintf("'%s'", item)
			if i != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(item)
		}
		buf.WriteString("]")

	default:
		// string literals and any other types will be treated as string and escaped
		actualValue = C.OidOutputFunctionCall(typeoutput, node.constvalue)

		// we need to check escaping here
		buf.WriteString(fmt.Sprintf("'%s'", C.GoString(actualValue)))

	}

}

// deparseFuncExpr: for a function call
// we use a hardcoded map to look for appropriate CH func name
func deparseFuncExpr(node *C.FuncExpr, ctx *deparseCtx) {
	var (
		tuple    C.HeapTuple
		form     C.Form_pg_proc
		procname string
		first    = true
	)
	buf := ctx.buf

	/* get the type's output function from system cache*/
	tuple = C.wrapper_SearchSysCache1(C.PROCOID, C.wrapper_ObjectIdGetDatum(node.funcid))
	if bool(!C.wrapper_HeapTupleIsValid(tuple)) {
		errLogger.Printf("cache lookup failed for function %v", node.funcid)
	}

	// we convert the HeapTuple pointer to pg_form_proc struct
	form = (C.Form_pg_proc)(unsafe.Pointer(C.wrapper_GETSTRUCT(tuple)))
	// extract function name
	// see NameStr for equiv macro in PG source
	procname = C.GoString(&form.proname.data[0])

	// map PG func to CH equivalent
	modprocname, ok := mapPGfuncToCH[procname]
	if !ok {
		errLogger.Printf("no support for %v", procname)
	}

	switch modprocname {
	case "__placeholder__":
		buf.WriteString(modprocname)
	default:
		buf.WriteString(fmt.Sprintf("%s(", modprocname))
	}

	// add arguments
	for cell := C.list_head(node.args); cell != nil; cell = C.wrapper_lnext(node.args, cell) {

		if !first {
			buf.WriteString(", ")
		}
		// assuming that we always get a list of `Expr`
		// not RestrictInfo's
		expr := (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(cell)))
		deparseExpr(expr, ctx)
		first = false
	}
	if modprocname == "__placeholder__" {
		// In short : we are mapping `date_part("month", timestamp(xx))` => `toMonth(toDateTime(xx))`
		// if we get a `date_part`
		// pick the argument for this function
		// and map to the appropriate CH function like toMonth, toYear etc.
		var (
			index    = -1
			matched  string
			funcName string
		)

		finalizedStr := buf.String()
		// split the original formatted buffer into two , there should always be two splits only
		splitted := strings.Split(finalizedStr, "__placeholder__")
		pre, funcStr := splitted[0], splitted[1]

		for _, v := range tsDurations {
			index = strings.Index(funcStr, v)
			if index != -1 {
				matched = v
				break
			}
		}
		// if no match found in available TS types
		if index == -1 {
			errLogger.Println("Unsupported extract operator for timestamp")
		}
		// get actual argument to date_part function
		// assuming date_part is always a 2 arg function try to remove the operator string (i.e map `month,` => ``)
		arg := strings.TrimSpace(strings.Replace(funcStr, matched+",", "", -1))

		switch matched {
		case "'month'":
			funcName = "toMonth"
		case "'year'":
			funcName = "toYear"
		case "'hour'":
			funcName = "toHour"
		case "'minute'":
			funcName = "toMinute"
		case "'second'":
			funcName = "toSecond"
		}
		// reset buffer
		buf.Reset()
		// rewrite with new formatted function
		buf.WriteString(pre + fmt.Sprintf("%s(%s", funcName, arg))
	}
	buf.WriteString(")")
	C.ReleaseSysCache(tuple)

}

// ScalarArrayOpExpr - expression node for "scalar op ANY/ALL (array)"
func deparseScalarArrayOpExpr(node *C.ScalarArrayOpExpr, ctx *deparseCtx) {
	var (
		tuple  C.HeapTuple
		form   C.Form_pg_operator
		opname string
		arg1   *C.Expr
		arg2   *C.Expr
	)
	buf := ctx.buf

	/* get the type's output function from system cache*/
	tuple = C.wrapper_SearchSysCache1(C.OPEROID, C.wrapper_ObjectIdGetDatum(node.opno))
	if bool(!C.wrapper_HeapTupleIsValid(tuple)) {
		errLogger.Printf("cache lookup failed for operator %v", node.opno)
	}

	// we convert the HeapTuple pointer to pg_form_operator struct
	form = (C.Form_pg_operator)(unsafe.Pointer(C.wrapper_GETSTRUCT(tuple)))

	if C.list_length(node.args) != C.int(2) {
		errLogger.Printf("Incorrect arg length for scalar op expr")
	}

	arg1 = (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(C.list_head(node.args))))
	arg2 = (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(C.wrapper_lnext(node.args, C.list_head(node.args)))))

	// deparse left operand
	deparseExpr(arg1, ctx)
	buf.WriteString(" ")

	opname = C.GoString(&form.oprname.data[0])
	// add opname
	if opname == "<>" {
		buf.WriteString(" NOT ")
	} else {
		// every other time the opname is going to be `=`
		// IN operator equates to `=`
		// buf.WriteString(fmt.Sprintf(" %s ", opname))
	}
	buf.WriteString("IN (")

	//add second operand
	switch C.wrapper_nodeTag((*C.Node)(unsafe.Pointer(arg2))) {
	case C.T_Const:
		// this means we have arrays of constants
		c := (*C.Const)(unsafe.Pointer(arg2))
		deparseConstArray(c, ctx)
	default:
		// otherwise parse expressions
		deparseExpr(arg2, ctx)
	}
	buf.WriteString(")")
	C.ReleaseSysCache(tuple)
}

// RelabelType represents a "dummy" type coercion between two binary-compatible datatypes
func deparseRelabelType(node *C.RelabelType, ctx *deparseCtx) {

	deparseExpr(node.arg, ctx)
}

// BoolExpr - expression node for the basic Boolean operators AND, OR, NOT
// arguments are given as a List.  For NOT, of course the list
// must always have exactly one element.  For AND and OR, there can be two
// or more arguments.
// args here are flattened into a list
func deparseBoolExpr(node *C.BoolExpr, ctx *deparseCtx) {

	var (
		op  string
		buf = ctx.buf
	)
	switch node.boolop {
	case C.AND_EXPR:
		op = "AND"
	case C.OR_EXPR:
		op = "OR"
	case C.NOT_EXPR:
		op = "NOT"
	}
	buf.WriteString(fmt.Sprintf(" (%s", op))

	// add arguments
	for cell := C.list_head(node.args); cell != nil; cell = C.wrapper_lnext(node.args, cell) {

		expr := (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(cell)))
		deparseExpr(expr, ctx)
	}

}

// deparse IS NULL , IS NOT NULL
func deparseNullTest(node *C.NullTest, ctx *deparseCtx) {
	var (
		op  string
		buf = ctx.buf
	)
	switch node.nulltesttype {
	case C.IS_NULL:
		op = "isNull"
	case C.IS_NOT_NULL:
		op = "isNotNull"
	default:
		errLogger.Printf("unknown null test type")
	}

	buf.WriteString(fmt.Sprintf("%s(", op))
	deparseExpr(node.arg, ctx)
	buf.WriteString(")")
}

// deparse ARRAY[...]
// note that in case of multidimensional arrays
// elements will be again ArrayExpr
func deparseArrayExpr(node *C.ArrayExpr, ctx *deparseCtx) {

	var (
		buf   = ctx.buf
		first = true
	)
	// construct CH arrays as [...]
	buf.WriteString("[")

	for cell := C.list_head(node.elements); cell != nil; cell = C.wrapper_lnext(node.elements, cell) {

		if !first {
			buf.WriteString(", ")
		}
		expr := (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(cell)))
		deparseExpr(expr, ctx)
		first = false
	}
	buf.WriteString("]")
}

// ADD when needed
func deparseCaseExpr(node *C.CaseExpr, ctx *deparseCtx) {

	errLogger.Printf("no support yet")
}

func deparseCoalesceExpr(node *C.CoalesceExpr, ctx *deparseCtx) {

	var (
		buf   = ctx.buf
		first = true
	)
	buf.WriteString("coalesce(")
	// add arguments
	for cell := C.list_head(node.args); cell != nil; cell = C.wrapper_lnext(node.args, cell) {

		if !first {
			buf.WriteString(", ")
		}
		expr := (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(cell)))
		deparseExpr(expr, ctx)
		first = false
	}
	buf.WriteString(")")
}

// NULLIF expression
func deparseNullIfExpr(node *C.NullIfExpr, ctx *deparseCtx) {

	var (
		buf = ctx.buf
	)

	buf.WriteString("nullIf(")
	// get first and second elements from the args list
	expr := (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(C.list_head(node.args))))
	deparseExpr(expr, ctx)
	expr = (*C.Expr)(unsafe.Pointer(C.wrapper_lfirst(C.list_tail(node.args))))
	deparseExpr(expr, ctx)
	buf.WriteString(")")
}

// see nodes.h for details on Aggref struct
// and pg_aggregate.h for details on various Aggregate types
func deparseAggref(node *C.Aggref, ctx *deparseCtx) {
	var (
		buf      = ctx.buf
		tuple    C.HeapTuple
		form     C.Form_pg_proc
		procname string
		tle      *C.TargetEntry
		first    = true
	)

	if node.aggsplit != C.AGGSPLIT_SIMPLE {
		errLogger.Println("only basic non-split aggregation are supported")
	}

	if node.aggvariadic != C.bool(false) {
		errLogger.Println("no variadic arguments support yet")
	}

	// get function name from OID of function name
	tuple = C.wrapper_SearchSysCache1(C.PROCOID, C.wrapper_ObjectIdGetDatum(node.aggfnoid))
	if bool(!C.wrapper_HeapTupleIsValid(tuple)) {
		errLogger.Printf("cache lookup failed for function %v", node.aggfnoid)
	}

	// we convert the HeapTuple pointer to pg_form_proc struct
	form = (C.Form_pg_proc)(unsafe.Pointer(C.wrapper_GETSTRUCT(tuple)))

	// PG_CATALOG_NAMESPACE = 11
	// we should check namespace for functions but constants are not available on CGO side
	// if form.pronamespace != 11 {
	// 	// otherwise we'll need to append schema name with function name
	// 	errLogger.Println("only pg_catalog namespace for functions is supported")
	// }

	// extract function name
	// see NameStr for equiv macro in PG source
	procname = C.GoString(&form.proname.data[0])

	// map PG func to CH equivalent
	name, ok := mapPGfuncToCH[procname]
	if !ok {
		errLogger.Printf("no support for %v", procname)
	}

	buf.WriteString(fmt.Sprintf("%s(", name))

	C.ReleaseSysCache(tuple)

	if node.aggdistinct != nil {
		errLogger.Println("DISTINCT with GROUP BY not supported yet")
	}

	// AGGKIND_NORMAL = 'n' see pg_aggregate.h (#defined)
	if node.aggkind != C.char('n') {
		errLogger.Println("ordered-set aggregates are not supported")
	}

	if node.aggstar == C.bool(true) {
		// '*' as argument
		buf.WriteString("*")

	} else {
		// extract arguments from list of node.args
		for cell := C.list_head(node.args); cell != nil; cell = C.wrapper_lnext(node.args, cell) {
			// get the targetEntry and get the expr
			tle = (*C.TargetEntry)(unsafe.Pointer(C.wrapper_lfirst(cell)))
			expr := (*C.Expr)(unsafe.Pointer(tle.expr))

			if tle.resjunk {
				continue
			}
			if !first {
				buf.WriteString(", ")
			}

			deparseExpr(expr, ctx)
			first = false
		}
	}

	// TODO: add order by support if passed from prev stages
	if node.aggorder != nil {
		// will need to implement dpearseAggOrder/appendAggOrderBy
	}

	if node.aggfilter != nil {
		errLogger.Println("no FILTER ( WHERE .. ) support yet")
	}

	buf.WriteString(")")

}

// we need this because const arrays are being represented
// as '{1, 2, 3...}' format in PG
// which we need to parse into proper arrays of CH (applicable for strings or numbers)
// this is almost same as deparseConst therefore
func deparseConstArray(node *C.Const, ctx *deparseCtx) {
	var (
		typeoutput   C.Oid
		typeIsVarLen C.bool
		actualValue  *C.char
	)
	buf := ctx.buf

	if bool(node.constisnull) {
		buf.WriteString(" NULL ")
		return
	}
	// extract type info of the constant into the typeoutput and typeIsVarLen
	C.getTypeOutputInfo(node.consttype, &typeoutput, &typeIsVarLen)

	// extract output values
	// remove leading and trailing '}' and '{'
	actualValue = C.OidOutputFunctionCall(typeoutput, node.constvalue)
	value := C.GoString(actualValue)
	value = strings.TrimLeft(value, "{")
	value = strings.TrimRight(value, "}")

	switch node.consttype {
	case C.INT2ARRAYOID:
		fallthrough
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
		// we're considering only these numeric arrays
		// so no escaping for them
		// values are already concat with ',' so just print them
		buf.WriteString(value)
	default:
		// otherwise quote every item of passed array (after removing'{' and '}')
		// standard lib doesn't provide a way for quoting with single quotes
		separated := strings.Split(value, ",")

		for i, item := range separated {
			item = fmt.Sprintf("'%s'", item)
			if i != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(item)
		}
	}
}

// extract TargetEntry from TargetList
// parse the exprs which in turn have Vars present is TargetEntry
// retrievedAttrs is the list of continuously increasing integers starting
// from 1. It has same number of entries as tlist.
func deparseExplicitTargetList(targetList *C.List, retrievedAttrs **C.List, ctx *deparseCtx) {
	var (
		buf   = ctx.buf
		index = 0
		tle   *C.TargetEntry
	)

	for cell := C.list_head(targetList); cell != nil; cell = C.wrapper_lnext(targetList, cell) {

		if index > 0 {
			buf.WriteString(",")
		}
		expr := (unsafe.Pointer(C.wrapper_lfirst(cell)))
		if C.wrapper_nodeTag((*C.Node)(expr)) == C.T_TargetEntry {
			tle = (*C.TargetEntry)(expr)
		} else {
			errLogger.Println("unexpected type in expr list for targetList")
		}
		// use deparseExpr to add Var names to buffer, this will even include any Function operated on Var names like SUM(x)..
		deparseExpr((*C.Expr)(unsafe.Pointer(tle.expr)), ctx)

		*retrievedAttrs = C.lappend_int(*retrievedAttrs, C.int(index+1))
		index++
	}

}

func appendGroupByClause(targetList *C.List, ctx *deparseCtx) {
	var (
		buf   = ctx.buf
		first = true
		// query is *C.Query struct
		query = ctx.root.parse
	)

	if query.groupClause == nil {
		return
	}
	// buf.WriteString("")

	// no groupingSets yet
	if query.groupingSets != nil {
		errLogger.Println("no support for groping sets")
	}

	for cell := C.list_head(query.groupClause); cell != nil; cell = C.wrapper_lnext(query.groupClause, cell) {

		if !first {
			buf.WriteString(", ")
		}
		grp := (*C.SortGroupClause)(unsafe.Pointer(C.wrapper_lfirst(cell)))

		deparseSortGroupClause(grp.tleSortGroupRef, targetList, ctx)
		first = false

	}
}

func deparseSortGroupClause(refno C.Index, targetList *C.List, ctx *deparseCtx) {
	var (
		buf = ctx.buf
		tle *C.TargetEntry
	)

	//Find the targetlist entry matching the given SortGroupRef index,
	//and return it.
	tle = C.get_sortgroupref_tle(refno, targetList)
	expr := tle.expr

	if expr == nil {
		return
	}

	if C.wrapper_nodeTag((*C.Node)(unsafe.Pointer(expr))) == C.T_Const {
		deparseConst((*C.Const)(unsafe.Pointer(expr)), ctx)
	} else if C.wrapper_nodeTag((*C.Node)(unsafe.Pointer(expr))) == C.T_Var {
		deparseExpr(expr, ctx)
	} else {
		// if not constant or plain columns
		// parenthesize
		buf.WriteString("(")
		deparseExpr(expr, ctx)
		buf.WriteString(")")
	}

}

func appendHaving(remoteConds *C.List, ctx *deparseCtx) {
	// remoteConds is a list of RestrictInfo structs so pick clause from them
	appendConditions(remoteConds, ctx)
}
