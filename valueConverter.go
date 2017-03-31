//
// Copyright 2017, TCN Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of TCN Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package sqlspanner

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	v1 "google.golang.org/genproto/googleapis/spanner/v1"
)

type valueConverter struct{}

func (v valueConverter) ConvertValue(src interface{}) (driver.Value, error) {
	return nil, errors.New(UnimplementedError)
}

func (v valueConverter) ConvertGenericCol(g *spanner.GenericColumnValue) (driver.Value, error) {
	if g == nil {
		return nil, nil
	}
	if g.Type == nil {
		return nil, fmt.Errorf("recieved a GenericColumnValue with a nil Type field")
	}
	switch g.Type.Code {
	case v1.TypeCode_TYPE_CODE_UNSPECIFIED:
		return nil, fmt.Errorf("GenericColumnValue type code unspecified")
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
		var val civil.Date
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_STRING:
		var val string
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_BYTES:
		var val []byte
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_ARRAY: // [](basic type)  or []struct
		if g.Type.ArrayElementType == nil {
			return nil, fmt.Errorf("Recieved array TypeCode with nil ArrayElementType")
		}
		return convertArrayType(g, g.Type.ArrayElementType)
	case v1.TypeCode_STRUCT: // unsupported
		return nil, errors.New(UnimplementedError)
	default:
	}
	return nil, nil
}

// recursively calls itself  till there is no more array type given
// it is recursive, because it is possible to have an array of array of arrays etc.
func convertArrayType(g *spanner.GenericColumnValue, arrType *v1.Type) (driver.Value, error) {
	if arrType == nil {
		return nil, fmt.Errorf("recieved nil pointer when converting GenericColumnValue")
	}
	switch arrType.Code {
	case v1.TypeCode_BOOL:
		var val []spanner.NullBool
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_INT64:
		var val []spanner.NullInt64
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_FLOAT64:
		var val []spanner.NullFloat64
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_BYTES:
		var val [][]byte
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_TIMESTAMP:
		var val []spanner.NullTime
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_STRING:
		var val []spanner.NullString
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_DATE:
		var val []spanner.NullDate
		g.Decode(&val)
		return val, nil
	case v1.TypeCode_STRUCT: //spanner.NullRow?
		_ = g.Value.GetListValue().GetValues()[0].GetStructValue().GetFields()
		// create []reflect.StructField from map["string"]*ptypes.Value
		return nil, errors.New(UnimplementedError)
	default:
		return nil, fmt.Errorf("not able to decoded type")
	}
}
