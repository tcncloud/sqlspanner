//
// Copyright 2017, TCN Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of TCN Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package sqlspanner

import (
	"cloud.google.com/go/spanner"
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"strings"
)

type stmt struct {
	conn            *conn
	parsedStatement sqlparser.Statement
	origQuery       string
	updatedQuery    string // for selects
	tableName       string
	columnNames     []string
	partialArgs     interface{}
	tce             *typeCacheEncoder
	currentCol      int
}

func newStmt(query string, c *conn) (driver.Stmt, error) {
	pstmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	st := &stmt{
		conn:            c,
		origQuery:       query,
		parsedStatement: pstmt,
		// stores the raw values for each column at the position as they are passed
		// to the statement. This gets rid of the restriction of driver.Value default types
		tce:             newTypeCacheEncoder(),
		// the current col in the row we are editing
		currentCol:      -1,
	}
	switch s := pstmt.(type) {
	case *sqlparser.Insert:
		pArgSlice, err := prepareInsertValues(s)
		if err != nil {
			return nil, err
		}
		columnNames, err := extractInsertColumns(s)
		if err != nil {
			return nil, err
		}
		tableName, err := extractIUDTableName(s)
		if err != nil {
			return nil, err
		}
		st.partialArgs = pArgSlice
		st.tableName = tableName
		st.columnNames = columnNames
	case *sqlparser.Update:
		pArgMap, err := extractUpdateClause(s)
		if err != nil {
			return nil, err
		}
		tableName, err := extractIUDTableName(s)
		if err != nil {
			return nil, err
		}
		st.partialArgs = pArgMap
		st.tableName = tableName
	case *sqlparser.Delete:
		mkr, err := extractSpannerKeyFromDelete(s)
		if err != nil {
			return nil, err
		}
		tableName, err := extractIUDTableName(s)
		if err != nil {
			return nil, err
		}
		st.partialArgs = mkr
		st.tableName = tableName
	case *sqlparser.Select:
		pArgMap := newPartialArgMap()
		spl := strings.Split(query, "?")
		for i := 0; i < len(spl)-1; i++ {
			namedIndex := fmt.Sprintf("@%d", i)
			st.updatedQuery += (spl[i] + namedIndex)
			pArgMap.AddArg(namedIndex, ArgPlaceholder{queuePos: i})
		}
		st.updatedQuery += spl[len(spl)-1]
		st.partialArgs = pArgMap
	}
	return st, nil
}

// sets the currentColumn we are editing and passes the statement back
// as a ValueConverter
func (s *stmt) ColumnConverter(idx int) driver.ValueConverter {
	s.currentCol = idx
	return s
}
// stores v as the value at the current index after confirming v
// can fit in spanner, and that the statements current column has
// been set to be used as a ColumnConverter.
func (s *stmt) ConvertValue(v interface{}) (driver.Value, error) {
	if s.currentCol == -1 {
		return nil, fmt.Errorf("cannot call ConvertValue without setting ColumnConvert index")
	}
	if IsValue(v) {
		if needsEncoding(v) {
			return s.tce.encodeCol(s.currentCol, v)
		}
		return v, nil
	}
	return nil, fmt.Errorf("value will not fit in spanner %#v", v)
}

// a statement doesnt have to do anything special to close it
func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	fmt.Printf("args now: %+v", args)
	args, err := s.getCachedArgs(args)
	if err != nil {
		return nil, err
	}
	switch s.parsedStatement.(type) {
	case *sqlparser.Insert:
		return s.executeInsertQuery(args)
	case *sqlparser.Update:
		return s.executeUpdateQuery(args)
	case *sqlparser.Delete:
		return s.executeDeleteQuery(args)
	default:
		return nil, fmt.Errorf("not a exec-able query")
	}
}


// creates a spanner statement out of the given query string and and array of driver values
// a spanner statment requires a Query with @ prefixed named args, instead of sql drivers ?
// and for params it requires a map[string]interface{} intead of []driver.Value
// Takes a query like: SELECT * FROM example_table WHERE a=?  OR b=? OR c=5
// and turns it into: SELECT * FROM example_table WHERE a=@1 OR b=@2 OR c=5
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	args, err := s.getCachedArgs(args)
	if err != nil {
		return nil, err
	}
	fmt.Printf("args now: %+v\n", args)
	_, ok := s.parsedStatement.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("not a query-able query (not a select statment)")
	}
	pArgMap, ok := s.partialArgs.(*partialArgMap)
	if !ok {
		return nil, fmt.Errorf("partialArgs was not a *partialArgMap.  Instead: %#v", s.partialArgs)
	}
	argsMap, err := pArgMap.GetFilledArgs(args)
	if err != nil {
		return nil, err
	}
	spannerStmt := spanner.Statement{SQL: s.updatedQuery, Params: argsMap}
	iter := s.conn.client.Single().Query(context.Background(), spannerStmt)

	return newRowsFromSpannerIterator(iter), nil
}

// pull out the args that are stored in stmt's typeCacheEncoder by the ConvertValue  function
//  driver.Statements are not used by multiple go routines concurrently
func (s *stmt) getCachedArgs(args []driver.Value) ([]driver.Value, error) {
	fmt.Println("in cached args")
	if s.currentCol != -1 {
		for i := 0; i < len(args); i++ {
			if bs, ok := args[i].([]byte); ok && s.tce.haveCol(i){
				arg, err := s.tce.decodeCol(i, bs)
				if err != nil {
					return nil, err
				}
				fmt.Printf("converting args[%d] to %+v\n", i, arg)
				args[i] = arg
			} else {
				fmt.Printf("already have arg[%d]: %#v\n", i, args[i])
			}
		}
		s.currentCol = -1
	}
	return args, nil
}

func (s *stmt) executeUpdateQuery(providedArgs []driver.Value) (driver.Result, error) {
	pArgMap, ok := s.partialArgs.(*partialArgMap)
	if !ok {
		return nil, fmt.Errorf("partialArgs was not a *partialArgMap.  Instead: %#v", s.partialArgs)
	}
	argsMap, err := pArgMap.GetFilledArgs(providedArgs)
	if err != nil {
		return nil, err
	}
	muts := make([]*spanner.Mutation, 1)
	muts[0] = spanner.UpdateMap(s.tableName, argsMap)
	_, err = s.conn.client.Apply(context.Background(), muts)

	rowsAffected := int64(1)
	return &result{
		lastID:       nil,
		rowsAffected: &rowsAffected,
	}, nil
}

func (s *stmt) executeDeleteQuery(providedArgs []driver.Value) (driver.Result, error) {
	mkr, ok := s.partialArgs.(*MergableKeyRange)
	if !ok {
		return nil, fmt.Errorf("partialArgs was not a *MergableKeyRange.  Instead: %#v", s.partialArgs)
	}
	keyRange, err := mkr.ToKeyRange(providedArgs)
	if err != nil {
		return nil, err
	}
	muts := make([]*spanner.Mutation, 1)
	muts[0] = spanner.DeleteKeyRange(s.tableName, *keyRange)
	_, err = s.conn.client.Apply(context.Background(), muts)
	if err != nil {
		return nil, err
	}
	// TODO: find actual number of rows affected
	rowsAffected := int64(1)
	return &result{
		lastID:       nil,
		rowsAffected: &rowsAffected,
	}, nil
}

func (s *stmt) executeInsertQuery(providedArgs []driver.Value) (driver.Result, error) {
	pArgSlice, ok := s.partialArgs.(*partialArgSlice)
	if !ok {
		return nil, fmt.Errorf("partialArgs was not a *partialArgSlice.  Instead: %#v", s.partialArgs)
	}
	args, err := pArgSlice.GetFilledArgs(providedArgs)
	if err != nil {
		return nil, err
	}
	// create a spanner mutation for the insert query
	muts := make([]*spanner.Mutation, 1)
	muts[0] = spanner.Insert(s.tableName, s.columnNames, args)
	// should probably support different contexts for querying spanner, inserts, deletes, and updates are slow
	_, err = s.conn.client.Apply(context.Background(), muts)
	if err != nil {
		return nil, err
	}
	//TODO:  find the last inserted id, and put it on the result
	rowsAffected := int64(1)
	return &result{
		lastID:       nil,
		rowsAffected: &rowsAffected,
	}, nil
}
