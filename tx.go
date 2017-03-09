package sqlspanner

import (
	"context"
	"database/sql/driver"
	"errors"
)

type tx struct {
	opts *driver.TxOptions
	c    *conn
	ctx  context.Context
}

func newTransaction(c *conn, ctx context.Context, opts *driver.TxOptions) (driver.Tx, error) {
	t := &tx{
		opts: opts,
		c:    c,
		ctx:  ctx,
	}
	return t, nil
}

// there is no Commit  in spanner, so this should just release its resources
func (t *tx) Commit() error {
	t = nil
	return nil
}

func (t *tx) Rollback() error {
	return errors.New(UnsupportedError)
}
