package sqlspanner

import (
	"database/sql/driver"
	"errors"
)

type stmt struct{}

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
