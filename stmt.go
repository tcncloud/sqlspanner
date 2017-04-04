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
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"github.com/xwb1989/sqlparser"
	"cloud.google.com/go/spanner"
)

type stmt struct {
	conn            *conn
	parsedStatement sqlparser.Statement
	origQuery       string
	updatedQuery    string// for selects
	tableName       string
	columnNames     []string
	partialArgs     interface{}
}

func newStmt(query string, c *conn) (driver.Stmt, error){
	pstmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	st := &stmt{
		conn: c,
		origQuery: query,
		parsedStatement: pstmt,
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
		if err !=nil {
			return nil, err
		}
		st.partialArgs = mkr
		st.tableName = tableName
	case *sqlparser.Select:
		pArgMap := &partialArgMap{}
		spl := strings.Split(query, "?")
		for i := 0; i < len(spl) - 1; i++ {
			namedIndex := fmt.Sprintf("@%d", i)
			st.updatedQuery += (spl[i] + namedIndex)
			pArgMap.AddArg(namedIndex, ArgPlaceholder{queuePos: i})
		}
		st.updatedQuery += spl[len(spl) - 1]
		st.partialArgs = pArgMap
	}
	return st, nil
}

func (s *stmt) Close() error {
	return errors.New(UnimplementedError)
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
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
	spannerStmt := spanner.Statement{ SQL: s.updatedQuery, Params: argsMap }
	iter := s.conn.client.Single().Query(context.Background(), spannerStmt)

	return newRowsFromSpannerIterator(iter), nil
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

	rowsAffected := int64(1)
	return &result{
		lastID: nil,
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
		lastID: nil,
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
