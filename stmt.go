package sqlspanner

import "database/sql/driver"

type stmt struct{}

func (s *stmt) Close() error {
	return unimplemented
}

func (s *stmt) NumInput() int {
	return 0
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, unimplemented
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, unimplemented
}