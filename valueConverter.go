package sqlspanner

import (
	"errors"
	"database/sql/driver"
)

type valueConverter struct{}

func (v valueConverter) ConvertValue(src interface{}) (driver.Value, error) {
	return nil, errors.New(UnimplementedError)
}
