package sqlspanner

import (
	"database/sql/driver"
	"fmt"
)

var unimplemented = fmt.Errorf("unimplemented")

type conn struct {
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return nil, unimplemented
}

func (c *conn) Close() error {
	return unimplemented
}

func (c *conn) Begin() (driver.Tx, error) {
	return nil, unimplemented
}
