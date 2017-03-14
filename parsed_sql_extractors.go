package sqlspanner

import(
	"fmt"
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

// extracts a valid table name for an insert query
// does not support:
// - empty table name ex. (INSERT INTO "" (...))
// - table name qualifiers ex. (INSERT INTO table_name as t1 (...))
func extractInsertTableName(insert *sqlparser.Insert) (*string, error) {
	if insert.Table == nil {
		return nil, fmt.Errorf("TableName node cannot be nil")
	}
	if len(insert.Table.Qualifier) != 0 {
		fmt.Printf("table qualifier: %s", string(insert.Table.Qualifier[:]))
		return nil, fmt.Errorf("Table Name Qualifiers are not supported for insert queries")
	}
	if len(insert.Table.Name) == 0 {
		return nil, fmt.Errorf("Table name cannot be empty for insert queries")
	}
	ret := string(insert.Table.Name[:])
	return &ret, nil
}

func extractInsertRows(insert *sqlparser.Insert) ([]string, error) {
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
		case sqlparser.ValTuple:
			fmt.Printf("is ValTuple %+v\n", valType)
			valExp := sqlparser.ValExprs(valType)
			valExps := ([]sqlparser.ValExpr)(valExp)
			//rowValues := make([]string, len(valExps))
			for _, ve := range valExps {
				switch ve.(type) {
				case sqlparser.StrVal:
					fmt.Printf("StrVal %+v\n",ve)
				case sqlparser.NumVal:
					fmt.Printf("NumVal %+v\n",ve)
				case sqlparser.ValArg:
					fmt.Printf("ValArg %+v\n",ve)
				case *sqlparser.NullVal:
					fmt.Printf("NullVal %+v\n",ve)
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
			return nil, nil
		}
		return nil, nil
	}
	return nil, nil
}

