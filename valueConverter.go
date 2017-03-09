package sqlspanner

import (
	"database/sql/driver"
)

type valueConverter struct{}

func (v valueConverter) ConvertValue(src interface{}) (driver.Value, error) {
	return nil, UnimplementedError
}
