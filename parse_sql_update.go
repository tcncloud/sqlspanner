package sqlspanner

import (
	"database/sql/driver"
	"fmt"
	"github.com/xwb1989/sqlparser"
)

type updateMap struct {
	updatedVals map[string]interface{}
	myArgs *Args
}

func extractUpdateClause(update *sqlparser.Update, args []driver.Value) (map[string]interface{}, error) {
	myArgs := &Args{Values: args}
	updatedVals := make(map[string]interface{})
	updateExprs := ([]*sqlparser.UpdateExpr)(update.Exprs)
	for _, updateExpr := range updateExprs {
		if updateExpr.Name == nil {
			return nil, fmt.Errorf("No column name associated with expression %+v", updateExpr.Expr)
		}
		if len(updateExpr.Name.Qualifier) > 0 {
			return nil, fmt.Errorf("qualifiers on column names not allowed for update clause")
		}
		if len(updateExpr.Name.Name) <= 0 {
			return nil, fmt.Errorf("No column name associated with expression %+v", updateExpr.Expr)
		}
		name := string(updateExpr.Name.Name[:])
		arg, err := myArgs.ParseValExpr(updateExpr.Expr)
		if err != nil {
			return nil, err
		}
		updatedVals[name] = arg
	}
	upMap := updateMap{updatedVals: updatedVals, myArgs: myArgs}
	err := upMap.walkBoolExpr(update.Where.Expr)
	if err != nil {
		return nil, err
	}
	// the changed values of the row are now in the map,  now we need to add the
	// primary key from the where clause.
	return upMap.updatedVals, nil
}

func (u *updateMap) walkBoolExpr(boolExpr sqlparser.BoolExpr) error {
	switch expr := boolExpr.(type) {
	case *sqlparser.AndExpr:
		err := u.walkBoolExpr(expr.Left)
		if err != nil {
			return err
		}
		err = u.walkBoolExpr(expr.Right)
		if err != nil {
			return err
		}
	case *sqlparser.ComparisonExpr:
		name, err := u.validColNameFromValExpr(expr.Left)
		if err != nil {
			return err
		}
		if expr.Operator != "=" {
			return fmt.Errorf("only =  operator is supported in update query's Where clause")
		}
		val, err := u.myArgs.ParseValExpr(expr.Right)
		if err != nil {
			return err
		}
		//passed all the tests,  put the value in the map
		u.updatedVals[name] = val
	case *sqlparser.NullCheck:
		name, err := u.validColNameFromValExpr(expr.Expr)
		if err != nil {
			return err
		}
		if expr.Operator != "is null" {
			return fmt.Errorf(`only "is null" checks are supported in update query's Where clause`)
		}
		u.updatedVals[name] = nil
	default:
		return fmt.Errorf("Unsupported Boolexpr, only support AndExpr, NullCheck, or ComparisonExpr with =")
	}
	return nil
}

func (u *updateMap) validColNameFromValExpr(expr sqlparser.ValExpr) (string, error) {
	col, ok := expr.(*sqlparser.ColName)
	if !ok {
		return "", fmt.Errorf("problem with converting ValExpr to ColName %+v", expr)
	}

	if len(col.Qualifier) > 0 {
		return "", fmt.Errorf("qualifiers not supported in update queries")
	}
	name := string(col.Name[:])
	if _, present := u.updatedVals[name]; present {
		return "", fmt.Errorf("update query's where clause cannot have a column that overrides a row being upated")
	}
	return name, nil
}
