package sqlspanner

import (
	"database/sql"
	"database/sql/driver"
)

type drv struct{}

func init() {
	sql.Register("spanner", &drv{})
}

func (d *drv) Open(name string) (driver.Conn, error) {
	return nil, unimplemented
}
