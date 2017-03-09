package sqlspanner

import (
	"context"
	"database/sql/driver"
	"fmt"

	"cloud.google.com/go/spanner"
)

var unimplemented = fmt.Errorf("unimplemented")

type conn struct {
	ctx    context.Context
	client *spanner.Client
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, unimplemented
}

func (c *conn) Close() error {
	return unimplemented
}

func (c *conn) Begin() (driver.Tx, error) {
	return newTransaction(c, context.Background(), nil)
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return newTransaction(c, ctx, opts)
}
