package sqlspanner

import (
	"database/sql/driver"
	"errors"

	"github.com/xwb1989/sqlparser"
)

type stmt struct {
	conn            *conn
	parsedStatement sqlparser.Statement
	origQuery       string
}

func (s *stmt) Close() error {
	return errors.New(UnimplementedError)
}

func (s *stmt) NumInput() int {
	return 0
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New(UnimplementedError)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New(UnimplementedError)
}
