package sqlspanner

import (
	"fmt"
	"time"
	"database/sql/driver"
	"errors"
	v1 "google.golang.org/genproto/googleapis/spanner/v1"
	"cloud.google.com/go/spanner"
	"reflect"
)

type valueConverter struct{}

func (v valueConverter) ConvertValue(src interface{}) (driver.Value, error) {
	return nil, errors.New(UnimplementedError)
}

func (v valueConverter) ConvertGenericCol(g *spanner.GenericColumnValue) (driver.Value, error) {
	fmt.Printf("g %+v", g)
	if g == nil {
		return nil, nil
	}
	if g.Type == nil {
		return nil, fmt.Errorf("recieved a GenericColumnValue with a nil Type field")
	}
	switch g.Type.Code {
	case v1.TypeCode_TYPE_CODE_UNSPECIFIED:
	case v1.TypeCode_BOOL:
		var val bool
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_INT64:
		var val int64
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_FLOAT64:
		var val float64
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_TIMESTAMP:
		var val time.Time
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_DATE:
		return nil, errors.New(UnimplementedError)
	case v1.TypeCode_STRING:
		var val string
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_BYTES:
		var val []byte
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_ARRAY:
		if g.Type.ArrayElementType == nil {
			return nil, fmt.Errorf("Recieved array TypeCode with nil ArrayElementType")
		}
		myArrType := setupArrayType(g.Type.Code, g.Type.ArrayElementType)
		val := reflect.New(myArrType).Interface()
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_STRUCT:
		return nil, errors.New(UnimplementedError)
	default:
	}
	return nil, nil
}
// recursively calls itself  till there is no more array type given
// it is recursive, because it is possible to have an array of array of arrays etc.
func setupArrayType(ty v1.TypeCode, arrType *v1.Type) reflect.Type {
	return reflect.TypeOf(errors.New(UnimplementedError))
}
