package sqlspanner

import(
	"encoding/gob"
	"bytes"
	"fmt"
	"reflect"
)

// Stores type information found about a specific column for a statement
// this type information is then used to help decode bytes into a value of the stored
// type.  This approach should be safer than caching the actual argument, because multiple
// go routines can use the same statement concurrently as all the goroutines make requests
// using the same column types.
type typeCacheEncoder struct {
	types map[int]reflect.Type
}

func newTypeCacheEncoder() *typeCacheEncoder {
	return &typeCacheEncoder{types: make(map[int]reflect.Type)}
}

func (t *typeCacheEncoder) encodeCol(i int, v interface{}) ([]byte, error) {
	if v == nil {
		t.types[i] = nil
		return nil, nil
	}

	typ := reflect.TypeOf(v)
	val, isSet := t.types[i]
	if isSet {
		if val != typ {
			return nil, fmt.Errorf("Cannot use two different types for same column on same prepared statement")
		}
	}
	t.types[i] = typ
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (t *typeCacheEncoder) decodeCol(i int, bs []byte) (interface{}, error) {
	typ, isSet := t.types[i]
	if !isSet {
		return nil, fmt.Errorf("cannot decode a column with an unset type")
	}
	if typ == nil {
		return nil, nil
	}
	out := reflect.New(typ)
	// if this doesnt work switch statement on all types that will fit in spanner
	buf := bytes.NewBuffer(bs)
	err := gob.NewDecoder(buf).DecodeValue(out)
	if err != nil {
		return nil, err
	}
	return out.Elem().Interface(), nil
}

