package sqlspanner

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/xwb1989/sqlparser"

	"cloud.google.com/go/spanner"
)

type conn struct {
	ctx    context.Context
	client *spanner.Client
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := sqlparser.Parse(query)
	switch stmt.(type) {
	case *sqlparser.Insert:
	case *sqlparser.Update:
	case *sqlparser.Delete:
	default:
	}

	if err != nil {
		return nil, err
	}
	return nil, errors.New(UnimplementedError)
}

func (c *conn) Close() error {
	return errors.New(UnimplementedError)
}

func (c *conn) Begin() (driver.Tx, error) {
	return newTransaction(c, context.Background(), nil)
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return newTransaction(c, ctx, &opts)
}
