package sqlspanner

import (
	"io"
	"cloud.google.com/go/spanner"
)
// This file exports our private functions for testing

var (
	NewRowsFromSpannerIterator = newRowsFromSpannerIterator
	NewRowsFromNextable = newRowsFromNextable
	NewRowsFromSpannerRow = newRowsFromSpannerRow
)
type TestNextable struct {
	cur int
	max int
}
func (n TestNextable) Next() (*spanner.Row, error) {
	if n.cur > n.max {
		return nil, io.EOF
	}
	n.cur++
	return spanner.NewRow([]string{"a", "b", "c"}, []interface{}{"1", "2", "3"})
}

func (n TestNextable) Stop() {
	n.cur = n.max
}

func NewTestNextable(iterations int) TestNextable {
	return TestNextable{ cur: 0, max: iterations }
}





