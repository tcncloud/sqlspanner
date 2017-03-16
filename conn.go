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

	switch pstmt.(type) {
	case *sqlparser.Insert:
		logrus.Debug("is an insert query")
		return c.executeInsertQuery(pstmt.(*sqlparser.Insert), args)
	case *sqlparser.Update:
	case *sqlparser.Delete:
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
	tableName, err := extractInsertOrUpdateTableName(insert)
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
		lastID: nil,
		rowsAffected: &rowsAffected,
	}, nil
}

