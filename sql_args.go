package sqlspanner

import (
	"database/sql/driver"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"strconv"
)

type Args struct {
	Values []driver.Value
	Cur    int
}

func (a *Args) Next() (interface{}, error) {
	if a.Cur >= len(a.Values) {
		return nil, fmt.Errorf("out of range")
	}
	a.Cur += 1
	return a.Values[a.Cur-1], nil
}

func (a *Args) ParseValExpr(expr sqlparser.ValExpr) (interface{}, error) {
	switch value := expr.(type) {
	case sqlparser.StrVal: // a quoted string
		fmt.Printf("StrVal %+v\n", value)
		return string(value[:]), nil
	case sqlparser.NumVal:
		fmt.Printf("NumVal %+v\n", value)
		rv, err := strconv.ParseInt(string(value[:]), 10, 64)
		if err != nil {
			rv, err := strconv.ParseFloat(string(value[:]), 64)
			if err != nil {
				return nil, fmt.Errorf("could not parse number value as int or float")
			}
			return rv, nil
		} else {
			return rv, nil
		}
	case sqlparser.ValArg: // a ?
		fmt.Printf("ValArg %+v\n", value)
		arg, err := a.Next()
		if err != nil {
			return nil, fmt.Errorf("not enough arguments suplied to match query")
		}
		return arg, nil
	case *sqlparser.NullVal:
		return nil, nil
	case *sqlparser.ColName:
		fmt.Printf("ColName %+v\n", value)
	case sqlparser.ValTuple:
		fmt.Printf("ValTuple %+v\n", value)
	case *sqlparser.Subquery:
		fmt.Printf("Subquery %+v\n", value)
	case sqlparser.ListArg:
		fmt.Printf("ListArg %+v\n", value)
	case *sqlparser.BinaryExpr:
		fmt.Printf("BinaryExpr %+v\n", value)
	case *sqlparser.UnaryExpr:
		fmt.Printf("UnaryExpr %+v\n", value)
	case *sqlparser.FuncExpr:
		fmt.Printf("FuncExpr %+v\n", value)
	case *sqlparser.CaseExpr:
		fmt.Printf("CaseExpr %+v\n", value)
	}
	return nil, fmt.Errorf("unsupported value expression")
}
