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

	"github.com/Sirupsen/logrus"
	"github.com/xwb1989/sqlparser"

	"cloud.google.com/go/spanner"
)

type conn struct {
	ctx    context.Context
	client *spanner.Client
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	pstmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	return &stmt{
		conn:            c,
		parsedStatement: pstmt,
		origQuery:       query,
	}, nil
}

func (c *conn) Close() error {
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return newTransaction(context.Background(), c, nil)
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return newTransaction(ctx, c, &opts)
}

func (c *conn) Ping(ctx context.Context) error {
	stmt := spanner.Statement{SQL: "SELECT 1"}
	iter := c.client.Single().Query(c.ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		return driver.ErrBadConn
	}

	var i int64
	if row.Columns(&i) != nil {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	logrus.WithFields(logrus.Fields{
		"query": query,
		"args":  args,
	}).Debug("Executing query")
	pstmt, err := sqlparser.Parse(query)
	if err != nil {

		return nil, err
	}

	switch stmt := pstmt.(type) {
	case *sqlparser.Insert:
		logrus.Debug("is an insert query")
		return c.executeInsertQuery(stmt, args)
	case *sqlparser.Update:
		return c.executeUpdateQuery(stmt, args)
	case *sqlparser.Delete:
		logrus.Debug("is a delete query")
		return c.executeDeleteQuery(stmt, args)
	default:
	}

	return nil, errors.New(UnimplementedError)
}

func (c *conn) executeInsertQuery(insert *sqlparser.Insert, args []driver.Value) (driver.Result, error) {
	logrus.WithField("stmt", insert).Debug("insert statement")
	colNames, err := extractInsertColumns(insert)
	if err != nil {
		return nil, err
	}
	logrus.WithField("cols", colNames).Debug("column names")
	tableName, err := extractIUDTableName(insert)
	if err != nil {
		return nil, err
	}
	logrus.WithField("tableName", tableName).Debug("table name")
	values, err := extractInsertValues(insert, args)
	if err != nil {
		return nil, err
	}
	logrus.WithField("values", values).Debug("values")
	// create a spanner mutation for the insert query
	muts := make([]*spanner.Mutation, 1)
	muts[0] = spanner.Insert(tableName, colNames, values)
	// should probably support different contexts for querying spanner, inserts, deletes, and updates are slow
	_, err = c.client.Apply(context.Background(), muts)
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

func (c *conn) executeDeleteQuery(del *sqlparser.Delete, args []driver.Value) (driver.Result, error) {
	logrus.WithField("stmt", del).Debug("delete statment")
	tableName, err := extractIUDTableName(del)
	if err != nil {
		return nil, err
	}
	logrus.WithField("tableName", tableName).Debug("table name")
	keyset, err := extractSpannerKeyFromDelete(del, args)
	if err != nil {
		return nil, err
	}
	logrus.WithField("keyset", keyset).Debug("keyset")
	muts := make([]*spanner.Mutation, 0)
	for _, key := range keyset.Keys {
		muts = append(muts, spanner.Delete(tableName, key))
	}
	for i, keyRange := range keyset.Ranges {
		logrus.WithFields(logrus.Fields{
			"i":     i,
			"Start": keyRange.Start,
			"End":   keyRange.End,
			"Kind":  keyRange.Kind,
		}).Debug("KEYRANGE")
		muts = append(muts, spanner.DeleteKeyRange(tableName, keyRange))
	}
	_, err = c.client.Apply(context.Background(), muts)
	if err != nil {
		return nil, err
	}
	rowsAffected := int64(1)
	return &result{
		lastID:       nil,
		rowsAffected: &rowsAffected,
	}, nil
}

func (c *conn) executeUpdateQuery(up *sqlparser.Update, args []driver.Value) (driver.Result, error) {
	logrus.WithField("stmt", up).Debug("update statement")
	tableName, err := extractIUDTableName(up)
	if err != nil {
		return nil, err
	}
	logrus.WithField("tableName", tableName).Debug("table name")

	upMap, err := extractUpdateClause(up, args)
	if err != nil {
		return nil, err
	}

	muts := make([]*spanner.Mutation, 1)
	muts[0] = spanner.UpdateMap(tableName, upMap)
	_, err = c.client.Apply(context.Background(), muts)
	if err != nil {
		return nil, err
	}

	rowsAffected := int64(1)
	return &result{
		lastID:       nil,
		rowsAffected: &rowsAffected,
	}, nil
}

