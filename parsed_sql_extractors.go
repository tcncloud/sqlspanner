package sqlspanner

import(
	"fmt"
	"strconv"
	"database/sql/driver"
	"github.com/xwb1989/sqlparser"
	"github.com/Sirupsen/logrus"
)

//  extracts the column names used in an insert query.  Does not support:
//	- * expressions ex. (INSERT INTO table_name (*))
//  - column names with with qualifiers  ex. (INSERT INTO table_name as t1 (t1.id, ..))
func extractInsertColumns(insert *sqlparser.Insert) ([]string, error) {
	columns := ([]sqlparser.SelectExpr)(insert.Columns)
	colNames := make([]string, len(columns))
	// cast columns to either starExpr, NonstarExpr
	for i, c := range columns {
		switch t := c.(type) {
		case *sqlparser.StarExpr:
			logrus.WithFields(logrus.Fields{
				"Star:": t,
				"i:": i,
			}).Debug("star expr")
			return nil, fmt.Errorf("cannot use type: sqlparser.StarExpr in insert query")
		case *sqlparser.NonStarExpr:
			logrus.WithFields(logrus.Fields{
				"NonStar:": t,
				"i:": i,
			}).Debug("nonstar expr")
			e, ok := t.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, fmt.Errorf("cannot use any other type besides *sqlparser.ColName in insertQuery")
			}
			if len(e.Qualifier) != 0 {
				return nil, fmt.Errorf("cannot use column name qualifiers in insert query")
			}
			colNames[i] = string(e.Name[:])
		default:
			// This shouldn't ever happen
			return nil, fmt.Errorf("unknown column type")
		}
	}
	return colNames, nil
}

// extracts a valid table name for an insert/update/delete query
// does not support:
// - empty table name ex. (INSERT INTO "" (...))
// - table name qualifiers ex. (INSERT INTO table_name as t1 (...))
func extractIUDTableName(st sqlparser.Statement) (string, error) {
	var table *sqlparser.TableName
	switch stmt := st.(type) {
	case *sqlparser.Insert:
		table = stmt.Table
	case *sqlparser.Update:
		table = stmt.Table
	case *sqlparser.Delete:
		table = stmt.Table
	default:
		return "", fmt.Errorf("not a insert/update statment")
	}
	if table == nil {
		return "", fmt.Errorf("TableName node cannot be nil")
	}
	if len(table.Qualifier) != 0 {
		fmt.Printf("table qualifier: %s", string(table.Qualifier[:]))
		return "", fmt.Errorf("Table Name Qualifiers are not supported for insert/update queries")
	}
	if len(table.Name) == 0 {
		return "", fmt.Errorf("Table name cannot be empty for insert/update queries")
	}
	return string(table.Name[:]), nil
}

// takes driver args, and an inset query,  and returns the arguments to insert query in spanner.
// ? values will be filled in with a value from args
// providing NULL will return a nil in the return interface
// does not support:
// - subqueries
// - lists (if you want to insert an array,  use ?, and provide the value yourself)
// - referencing other columns
// - tuples
// - Binary, Unary, Function, or Case expressions
func extractInsertValues(insert *sqlparser.Insert, args []driver.Value) ([]interface{}, error) {
	rows := insert.Rows
	switch rowType := rows.(type) {
	case *sqlparser.Select, *sqlparser.Union:
		return nil, fmt.Errorf("insert queries must use simple values (No SELECTS, or UNIONs)")
	case sqlparser.Values:
		rowTuple := ([]sqlparser.RowTuple)(rowType)
		if len(rowTuple) != 1 {
			return nil, fmt.Errorf("Cannot use multiple row tuples for insert queries")
		}
		rt := rowTuple[0]
		switch valType := rt.(type) {
		case *sqlparser.Subquery:
			return nil, fmt.Errorf("insert queries cannot have subqueries")
		case sqlparser.ValTuple:// a number
			fmt.Printf("is ValTuple %+v\n", valType)

			valExp := sqlparser.ValExprs(valType)
			valExps := ([]sqlparser.ValExpr)(valExp)
			rowValues := make([]interface{}, len(valExps))

			curArg := 0

			for i, ve := range valExps {
				switch value := ve.(type) {
				case sqlparser.StrVal:// a quoted string
					rowValues[i] = string(value[:])
				case sqlparser.NumVal:
					rv, err := strconv.ParseInt(string(value[:]), 10, 64)
					if err != nil {
						rv, err := strconv.ParseFloat(string(value[:]), 64)
						if err != nil {
							return nil, fmt.Errorf("could not parse number value as int or float")
						}
						rowValues[i] = rv
					} else {
						rowValues[i] = rv
					}
				case sqlparser.ValArg:// a ?
					if curArg >= len(args) {
						return nil, fmt.Errorf("not enough arguments suplied to match query")
					}
					rowValues[i] = args[curArg]
					curArg++
				case *sqlparser.NullVal:
					rowValues[i] = nil
				case *sqlparser.ColName:
					fmt.Printf("ColName %+v\n",ve)
				case sqlparser.ValTuple:
					fmt.Printf("ValTuple %+v\n",ve)
				case *sqlparser.Subquery:
					fmt.Printf("Subquery %+v\n",ve)
				case sqlparser.ListArg:
					fmt.Printf("ListArg %+v\n",ve)
				case *sqlparser.BinaryExpr:
					fmt.Printf("BinaryExpr %+v\n",ve)
				case *sqlparser.UnaryExpr:
					fmt.Printf("UnaryExpr %+v\n",ve)
				case *sqlparser.FuncExpr:
					fmt.Printf("FuncExpr %+v\n",ve)
				case *sqlparser.CaseExpr:
					fmt.Printf("CaseExpr %+v\n",ve)
				}
			}
			return rowValues, nil
		}
	}
	return nil, fmt.Errorf("insert query not compatable with spanner insert")
}

