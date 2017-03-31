package sqlspanner

import (
	"cloud.google.com/go/spanner"
	"database/sql/driver"
	"fmt"
	"github.com/xwb1989/sqlparser"
)

// because spanner has multiple primary keys support,  EVERY field
// found in the query is assumed to be a primary key.  It will build the spanner.Key with the fields in the
// query in the order they are discovered. For Example if i have two queries:
//    q1 = "DELETE FROM test_table WHERE id = 1 AND simple_string="test_string"
//    q2 = "DELETE FROM test_table WHERE simple_string="test_string" AND id = 1
//    q1 would produce key: { 1, "test_string" }
//    q2 would produce key: { "test_string", 2 }
// Try to construct your queries with ANDs  instead of ORs. Because different fields are
// interpreted as primary keys,  it gets too difficult to parse what is meant by queries like:
//    q1 = "DELETE FROM test_table WHERE (id < 1 OR id >= 10) AND simple_string = "test_string"
// it might be possible in the future to parse the meaning of statements like this,  but for now it was
// easier to just drop support of statements for OR expressions
//   Other Rules:
// - NOT expressions are not supported, It is not possible to tell a spanner key what "not"  means.
// - currently only one key range, per primary key is permitted.  Just use two queries. ex.
//    not permitted: DELETE FROM test_table WHERE id > 1 AND id < 10 AND id > 20 AND id < 100
// - Does not support cross table queries
func extractSpannerKeyFromDelete(del *sqlparser.Delete, args []driver.Value) (*spanner.KeySet, error) {
	where := del.Where
	if where == nil {
		return nil, fmt.Errorf("Must include a where clause that contain primary keys in delete statement")
	}
	myArgs := &Args{Values: args}
	fmt.Printf("where type: %+v\n", where.Type)
	aKeySet := &AwareKeySet{
		Args:     myArgs,
		Keys:     make(map[string]*Key),
		KeyOrder: make([]string, 0),
	}
	err := aKeySet.walkBoolExpr(where.Expr)
	if err != nil {
		return nil, err
	}
	return aKeySet.packageKeySet()
}

type Key struct {
	Name       string
	LowerValue interface{}
	LowerOpen  bool
	UpperValue interface{}
	UpperOpen  bool
	HaveLower  bool
	HaveUpper  bool
}

type AwareKeySet struct {
	Keys     map[string]*Key
	KeyOrder []string
	Args     *Args
}

type MergableKeyRange struct {
	Start     []interface{}
	End       []interface{}
	LowerOpen bool
	UpperOpen bool
	HaveLower bool
	HaveUpper bool
}

// all lower bounds are turned into a key together.
// all upper bounds are turned into a key together.
// it is expected that all fields in a query belong together
func (a *AwareKeySet) packageKeySet() (*spanner.KeySet, error) {
	var prev *MergableKeyRange
	//makes sure all we dont have holes in our key ranges,  that is undefined behaviour
	for i := len(a.KeyOrder) - 1; i > 0; i-- { // dont check before the first elem
		me := a.Keys[a.KeyOrder[i]]
		keyBeforeMe := a.Keys[a.KeyOrder[i-1]]
		if me.HaveLower {
			if !keyBeforeMe.HaveLower {
				return nil, fmt.Errorf("cannot have a lower bound on a key range without defining all higher priority lower bounds")
			}
		}
		if me.HaveUpper {
			if !keyBeforeMe.HaveUpper {
				return nil, fmt.Errorf("cannot have a upper bound on a key range without defining all higher priority upper bounds")
			}
		}
	}
	for _, k := range a.KeyOrder {
		key := a.Keys[k]
		m := &MergableKeyRange{}
		m.fromKey(key)
		if prev == nil {
			prev = m
		} else {
			fmt.Printf("key that will populate m: %#v\n\n", key)
			fmt.Printf("populated m: %#v\n\n", m)
			err := prev.mergeKeyRange(m)
			fmt.Printf("merged prev with m %#v\n\n", prev)
			if err != nil {
				return nil, err
			}
		}
	}
	fmt.Printf("prev: %#v\n\n", prev)
	return prev.ToKeySet(), nil
}

func (k1 *MergableKeyRange) fromKey(key *Key) {
	if key == nil {
		return
	}
	k1.LowerOpen = key.LowerOpen
	k1.UpperOpen = key.UpperOpen
	k1.HaveLower = key.HaveLower
	k1.HaveUpper = key.HaveUpper
	k1.Start = []interface{}{key.LowerValue}
	k1.End = []interface{}{key.UpperValue}
}

func (k *MergableKeyRange) ToKeySet() *spanner.KeySet {
	keySet := &spanner.KeySet{}
	low := k.LowerOpen
	up := k.UpperOpen

	var kind spanner.KeyRangeKind

	if low && up {
		kind = spanner.OpenOpen
	} else if low && !up {
		kind = spanner.OpenClosed
	} else if !low && up {
		kind = spanner.ClosedOpen
	} else {
		kind = spanner.ClosedClosed
	}
	keySet.Ranges = append(keySet.Ranges, spanner.KeyRange{
		Start: spanner.Key(k.Start),
		End:   spanner.Key(k.End),
		Kind:  kind,
	})
	return keySet
}

func (k1 *MergableKeyRange) mergeKeyRange(k2 *MergableKeyRange) error {
	fmt.Printf("\nmerging into k1: %#v\n  k2: %#v\n\n", k1, k2)
	if k1 == nil && k2 != nil {
		*k1 = *k2
		return nil
	} else if k2 == nil {
		return nil
	}
	if k2.HaveLower {
		if k1.LowerOpen != k2.LowerOpen {
			return fmt.Errorf("Kinds in ranges must all match")
		}
		k1.Start = append(k1.Start, k2.Start...)
	}
	if k2.HaveUpper {
		if k1.UpperOpen != k2.UpperOpen {
			return fmt.Errorf("Kinds in ranges must all match")
		}
		k1.End = append(k1.End, k2.End...)
	}
	return nil
}

func (a *AwareKeySet) addKeyFromValExpr(valExpr sqlparser.ValExpr) (*Key, error) {
	col, ok := valExpr.(*sqlparser.ColName)
	if !ok {
		return nil, fmt.Errorf("not a valid column name")
	}
	if len(col.Qualifier) != 0 {
		return nil, fmt.Errorf("qualifiers not allowed")
	}
	keyName := string(col.Name[:])
	if a.Keys[keyName] == nil {
		a.KeyOrder = append(a.KeyOrder, keyName)
		a.Keys[keyName] = &Key{Name: keyName}
	}
	return a.Keys[keyName], nil
}

func (a *AwareKeySet) walkBoolExpr(boolExpr sqlparser.BoolExpr) error {
	switch expr := boolExpr.(type) {
	case *sqlparser.AndExpr:
		fmt.Printf("AndExpr %#v\n", expr)
		err := a.walkBoolExpr(expr.Left)
		if err != nil {
			return err
		}
		err = a.walkBoolExpr(expr.Right)
		if err != nil {
			return err
		}
		return nil
	case *sqlparser.OrExpr:
		fmt.Printf("OrExpr %#v\n", expr)
		return fmt.Errorf("Or Expressions are not currently supported")
	case *sqlparser.ParenBoolExpr:
		fmt.Printf("ParenBoolExpr %#v\n", expr)
	case *sqlparser.ComparisonExpr:
		fmt.Printf("ComparisonExpr %#v\n", expr)
		myKey, err := a.addKeyFromValExpr(expr.Left)
		if err != nil {
			return err
		}
		val, err := a.Args.ParseValExpr(expr.Right)
		if err != nil {
			return err
		}
		fmt.Printf("OPERTATOR %#v\n", expr.Operator)
		switch expr.Operator {
		case "=":
			myKey.LowerValue = val
			myKey.UpperValue = val
			myKey.LowerOpen = false
			myKey.UpperOpen = false
			myKey.HaveUpper = true
			myKey.HaveLower = true
			return nil
		case ">":
			myKey.LowerValue = val
			myKey.LowerOpen = true
			myKey.HaveLower = true
			return nil
		case "<":
			myKey.UpperValue = val
			myKey.UpperOpen = true
			myKey.HaveUpper = true
			return nil
		case ">=":
			myKey.LowerValue = val
			myKey.LowerOpen = false
			myKey.HaveLower = true
			return nil
		case "<=":
			myKey.UpperValue = val
			myKey.UpperOpen = false
			myKey.HaveUpper = true
			return nil
		case "!=":
			return fmt.Errorf("!= comparisons are not supported")
		case "not in", "in":
			return fmt.Errorf("in, and not in  comparisons are not supported")
		default:
			return fmt.Errorf("%#v  is not a supported operator", expr.Operator)
		}
	case *sqlparser.RangeCond:
		fmt.Printf("RangeCond %#v\n", expr)
		myKey, err := a.addKeyFromValExpr(expr.Left)
		if err != nil {
			return err
		}
		from, err := a.Args.ParseValExpr(expr.From)
		if err != nil {
			return err
		}
		to, err := a.Args.ParseValExpr(expr.To)
		if err != nil {
			return err
		}
		switch expr.Operator {
		case "between":
			myKey.LowerValue = from
			myKey.LowerOpen = true
			myKey.UpperValue = to
			myKey.UpperOpen = true
		case "not between":
			return fmt.Errorf("not between operator is not supported")
		}
	case *sqlparser.ExistsExpr:
		fmt.Printf("ExistsExpr %#v\n", expr)
		return fmt.Errorf("Exists Expressions are not supported")
	default:
		fmt.Printf("HITTING DEFAULT %#v\n", boolExpr)
	}

	return fmt.Errorf("not a boolexpr %#v\n", boolExpr)
}
