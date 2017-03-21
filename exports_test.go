package sqlspanner

import (
	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"database/sql/driver"
	"google.golang.org/api/iterator"
	"time"
)

// This file exports our private functions for testing

var (
	NewRowsFromSpannerIterator = newRowsFromSpannerIterator
	NewRowsFromNextable        = newRowsFromNextable
	NewRowsFromSpannerRow      = newRowsFromSpannerRow
)

type TestNextable struct {
	cur  int
	max  int
	name string
	now  time.Time
}

func (n *TestNextable) Next() (*spanner.Row, error) {
	if n.cur < n.max {
		n.cur += 1
		switch n.name {
		case "values":
			return n.valueRow()
		case "times":
			return n.timeRow()
		case "structs":
			return n.structRow()
		default:
			return n.valueRow()
		}
		//messyStr := [][]string{ []string { "123", "456" }, []string{ "78", "910" } }
		//return spanner.NewRow([]string{"a", "b", "c"}, []interface{}{"1", "2", messyStr })
	}
	return nil, iterator.Done
}

//test struct type for getting array of structs from spanner
type TestStruct struct {
	a string
	b string
}

func (n *TestNextable) structRow() (*spanner.Row, error) {
	return spanner.NewRow([]string{"a"}, []interface{}{
		[]TestStruct{
			TestStruct{a: "a", b: "b"},
			TestStruct{a: "a", b: "b"},
		},
	})
}

func (n *TestNextable) timeRow() (*spanner.Row, error) {
	return spanner.NewRow([]string{"a", "b", "c", "d"}, []interface{}{
		civil.DateOf(n.now), n.now,
		[]civil.Date{civil.DateOf(n.now), civil.DateOf(n.now)},
		[]time.Time{n.now, n.now},
	})
}
func (n *TestNextable) WhatTimeRowShouldBe() []driver.Value {
	return []driver.Value{civil.DateOf(n.now), n.now,
		[]spanner.NullDate{
			spanner.NullDate{Date: civil.DateOf(n.now), Valid: true},
			spanner.NullDate{Date: civil.DateOf(n.now), Valid: true},
		},
		[]spanner.NullTime{
			spanner.NullTime{Time: n.now, Valid: true},
			spanner.NullTime{Time: n.now, Valid: true},
		},
	}
}

func (n *TestNextable) valueRow() (*spanner.Row, error) {
	return spanner.NewRow([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
		[]interface{}{true, int64(2), float64(3.3), "1", []byte("bytes"),
			[]bool{true, true}, []int64{2, 2}, []float64{3.3, 3.3},
			[]string{"1", "1"}, [][]byte{[]byte("bytes"), []byte("bytes")}})
}

func (n *TestNextable) WhatValueRowShouldBe() []driver.Value {
	return []driver.Value{true, int64(2), float64(3.3), "1",
		[]byte("bytes"), []spanner.NullBool{
			spanner.NullBool{Bool: true, Valid: true},
			spanner.NullBool{Bool: true, Valid: true},
		}, []spanner.NullInt64{
			spanner.NullInt64{Int64: int64(2), Valid: true},
			spanner.NullInt64{Int64: int64(2), Valid: true},
		}, []spanner.NullFloat64{
			spanner.NullFloat64{Float64: float64(3.3), Valid: true},
			spanner.NullFloat64{Float64: float64(3.3), Valid: true},
		}, []spanner.NullString{
			spanner.NullString{StringVal: "1", Valid: true},
			spanner.NullString{StringVal: "1", Valid: true},
		}, [][]byte{
			[]byte("bytes"),
			[]byte("bytes"),
		},
	}
}

func (n *TestNextable) Stop() {
	n.cur = n.max
}

func NewTestNextable(iterations int, name string) *TestNextable {
	return &TestNextable{cur: 0, max: iterations, name: name, now: time.Now().UTC().Truncate(time.Millisecond)}
}
