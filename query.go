package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// stringSlice supports operations on a slice of string
type stringSlice []string

// Apply applies func f for each elements
func (ss stringSlice) Apply(f func(in string) string) stringSlice {
	res := make([]string, len(ss))
	for i, s := range ss {
		res[i] = f(s)
	}

	return res
}

func (ss stringSlice) TrimSpace() stringSlice {
	return ss.Apply(strings.TrimSpace)
}

func (ss stringSlice) TrimPrefix(prefix string) stringSlice {
	return ss.Apply(func(in string) string {
		return strings.TrimPrefix(in, prefix)
	})
}

func (ss stringSlice) Dots2Underscores() stringSlice {
	return ss.Apply(func(in string) string {
		return strings.Replace(in, ".", "_", -1)
	})
}

func (ss stringSlice) Contains(s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func (ss stringSlice) MinusSlice(b []string) stringSlice {
	res := make([]string, 0)
	for _, s := range ss {
		if stringSlice(b).Contains(s) == false {
			res = append(res, s)
		}
	}
	return res
}

func (ss stringSlice) Deduplicate() stringSlice {
	res := []string{}
	seen := map[string]bool{}
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			res = append(res, s)
			seen[s] = true
		}
	}
	return res
}

func (ss stringSlice) RemoveItem(r string) stringSlice {
	for i, v := range ss {
		if v == r {
			return append(ss[:i], ss[i+1:]...)
		}
	}
	return ss
}

// Value converts a stringSlice to []string
func (ss stringSlice) Value() []string {
	return []string(ss)
}

// Copy copies ss
func (ss stringSlice) Copy() stringSlice {
	cp := make([]string, len(ss))
	copy(cp, ss)
	return cp
}

// Sort returns a sorted (in ascending order) copy of ss
func (ss stringSlice) Sort() stringSlice {
	cp := ss.Copy()
	sort.Strings(cp)
	return cp
}

// Query represents a query to Clickhouse
type Query struct {
	SelectFields []string
	From         string
	FromUnion    []*Query
	FromSubQuery *Query
	Conditions   []string
	OrderBys     []string
	GroupBys     []string
	Having       []string
	LimitOneBy   []string
	Limit        uint64
	Offset       uint64
	Parameters   []interface{}

	// PreconditionFields Fields which to add to PREWHERE
	PreconditionFields []string
	// Conditions that are added to PREWHERE statement
	PreWheresConditions []string
	// Settings ClickHouse query settings
	Settings []string
}

func (q *Query) String() string {
	slect := "SELECT " + strings.Join(stringSlice(q.SelectFields).Deduplicate(), ",\n\t")
	var from string
	if q.From != "" {
		from = "FROM " + q.From
	} else if len(q.FromUnion) > 0 {
		union := []string{}
		for _, u := range q.FromUnion {
			union = append(union, u.String())
		}
		from = "FROM (\n" + strings.Join(union, "\n \n UNION ALL \n \n") + "\n) \n"
	} else if q.FromSubQuery != nil {
		from = "FROM (\n " + q.FromSubQuery.String() + " )"
	}

	where := ""
	preWhere := ""
	if len(q.Conditions) > 0 {
		q.Conditions = stringSlice(q.Conditions).Deduplicate()
		for _, cond := range q.Conditions {
			condField := strings.Split(cond, " ")[0]

			if stringSlice(q.PreconditionFields).Contains(condField) {
				q.PreWheresConditions = append(q.PreWheresConditions, cond)
			}
		}

		q.Conditions = stringSlice(q.Conditions).MinusSlice(q.PreWheresConditions)

		if len(q.Conditions) > 0 {
			where = "WHERE " + strings.Join(q.Conditions, " AND ")
		}
	}

	if len(q.PreWheresConditions) > 0 {
		q.PreWheresConditions = stringSlice(q.PreWheresConditions).Deduplicate()
		preWhere = "PREWHERE " + strings.Join(q.PreWheresConditions, " AND ")
	}

	orderBy := ""
	if len(q.OrderBys) > 0 {
		orderBy = "ORDER BY " + strings.Join(q.OrderBys, ", ")
	}

	groupBy := ""
	if len(q.GroupBys) > 0 {
		q.GroupBys = stringSlice(q.GroupBys).Deduplicate()
		groupBy = "GROUP BY " + strings.Join(q.GroupBys, ", ")
	}

	having := ""
	if len(q.Having) > 0 {
		q.Having = stringSlice(q.Having).Deduplicate()
		having = "HAVING " + strings.Join(q.Having, " AND ")
	}

	limitOneBy := ""
	if len(q.LimitOneBy) > 0 {
		limitOneBy = "LIMIT 1 BY " + strings.Join(q.LimitOneBy, ", ")
	}

	limit := ""
	if q.Limit != 0 {
		if q.Offset != 0 {
			limit = fmt.Sprintf("LIMIT %d, %d", q.Offset, q.Limit)
		} else {
			limit = "LIMIT " + strconv.FormatUint(q.Limit, 10)
		}
	}

	settings := ""
	if len(q.Settings) > 0 {
		settings = "SETTINGS " + strings.Join(q.Settings, ", ")
	}

	parts := []string{slect, from, preWhere, where, groupBy, having, orderBy, limitOneBy, limit, settings}
	return strings.Join(parts, "\n ")
}

func (q *Query) sanitizeOrderBys() {
	if len(q.OrderBys) > 0 {
		q.OrderBys = stringSlice(q.OrderBys).Deduplicate()

		for i, orderBy := range q.OrderBys {
			q.OrderBys[i] = strings.Replace(orderBy, "ORDER_BY_DIRECTION_", "", 1)
		}
	}
}
