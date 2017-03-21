package sqlspanner

import (
	"cloud.google.com/go/spanner"
	"database/sql/driver"
	"google.golang.org/api/iterator"
	"io"
)

type rows struct {
	iter   nextable
	row    *spanner.Row
	valuer valueConverter
	cols   []string
	err    error
}

func newRowsFromSpannerIterator(iter *spanner.RowIterator) *rows {
	return newRowsFromNextable(iter)
}

func newRowsFromNextable(iter nextable) *rows {
	r := &rows{
		iter:   iter,
		valuer: valueConverter{},
		row:    nil,
	}

	if r.iter == nil {
		r.err = io.EOF
		return r
	}

	r.iterate()
	if r.err == nil {
		r.cols = r.row.ColumnNames()
	}
	return r
}

func newRowsFromSpannerRow(row *spanner.Row) *rows {
	if row == nil {
		return newRowsFromNextable(nil)
	}
	return &rows{
		iter:   nil,
		valuer: valueConverter{},
		row:    row,
		cols:   row.ColumnNames(),
	}
}

func (r *rows) iterate() {
	if r.iter == nil {
		r.err = io.EOF
	}
	if r.err == nil {
		//get the first row result now, so we can get the column names
		// r.row only will exist if r.done is false
		row, err := r.iter.Next()
		if err == iterator.Done {
			r.err = io.EOF
		} else if err != nil {
			r.err = err
		} else {
			r.row = row
		}
	}
}

// because of how Next, and iterate work,  r.row  will always exist at this point
func (r *rows) handleRow(dest []driver.Value) {
	row := r.row
	r.row = nil
	//set up generic pointers to pull of each row
	pointers := make([]*spanner.GenericColumnValue, row.Size())
	for i := 0; i < row.Size(); i++ {
		pointers[i] = new(spanner.GenericColumnValue)
	}
	// convert pointers to []interface{}
	interfaces := make([]interface{}, len(pointers))
	for i, v := range pointers {
		interfaces[i] = v
	}
	//read all the columns from the row
	row.Columns(interfaces...)
	for i, col := range pointers {
		driverVal, err := r.valuer.ConvertGenericCol(col)
		if err != nil {
			// abort everything ever for this iterator
			r.err = err
			return
		}
		//dest is the same size as columns, so we dont need to append
		dest[i] = driverVal
	}
}

func (r *rows) Next(dest []driver.Value) error {
	switch {
	case r.row != nil: // first row gotten by an interator will be handled this way
		r.handleRow(dest)
		return r.err
	case r.err != nil: // always return the last error we were given
		return r.err
	default:
		r.iterate()
		if r.err != nil {
			return r.err
		}
		r.handleRow(dest)
		return r.err
	}
}

func (r *rows) Columns() []string {
	return r.cols
}

func (r *rows) Close() error {
	if r.iter != nil {
		r.iter.Stop()
	}
	return nil
}

// driver.Rows  is casted to driver.RowsNextResultSet  in the database/sql lib
// so even though spanner doesn't have multiple result sets, we have to implment this here.
func (r *rows) HasNextResultSet() bool {
	return false
}

func (r *rows) NextResultSet() error {
	return nil
}

// So I don't have to be given a spanner.RowIterator,  I could be given anything
// that returns a spanner.Row, and an error
type nextable interface {
	Next() (*spanner.Row, error)
	Stop()
}
