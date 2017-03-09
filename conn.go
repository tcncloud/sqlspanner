package sqlspanner

import (
	"context"
	"database/sql/driver"

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
