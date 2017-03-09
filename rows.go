package sqlspanner

import (
	"cloud.google.com/go/spanner"
	"database/sql/driver"
	"google.golang.org/api/iterator"
	"io"
)

type rows struct {
	iter   *spanner.RowIterator
	row    *spanner.Row
	valuer valueConverter
	cols   []string
	done   bool
	err    error
}

// Some of our methods we support require having a spanner row already
// this function sets this up so we can know the column names
func newRowsFromSpannerIterator(iter *spanner.RowIterator) *rows {
	r := &rows{
		iter:   iter,
		done:   false,
		valuer: valueConverter{},
	}

	if r.iter == nil {
		r.done = true
		return r
	}
	r.iterate()
	if !r.done {
		r.cols = r.row.ColumnNames()
	}
	return r
}

func newRowsFromSpannerRow(row *spanner.Row) *rows {
	//UNIMPLEMENTED
	return nil
}

func (r *rows) iterate() {
	if !r.done {
		//get the first row result now, so we can get the column names
		// r.row only will exist if r.done is false
		row, err := r.iter.Next()
		if err == iterator.Done {
			r.done = true
			r.err = io.EOF
		} else if err != nil {
			r.err = err
			r.done = true
		} else {
			r.row = row
		}
	}
}

// because of how Next, and iterate work,  r.row  will always exist at this point
func (r *rows) handleRow(dest []driver.Value) {
	//set up generic pointers to pull of each row
	pointers := make([]*spanner.GenericColumnValue, r.row.Size())
	for i := 0; i < r.row.Size(); i++ {
		pointers[i] = new(spanner.GenericColumnValue)
	}
	//read all the columns from the row
	r.row.Columns(pointers)
	for i, col := range pointers {
		driverVal, err := r.valuer.ConvertValue(col)
		if err != nil {
			// abort everything ever for this iterator
			r.err = err
			r.done = true
			r.row = nil
			return
		}
		//dest is the same size as columns, so we dont need to append
		dest[i] = driverVal
	}
	// nil out the row, because it has been assigned to dest
	r.row = nil
}

func (r *rows) Next(dest []driver.Value) error {
	switch {
	case r.done: // always return the last error we were given
		return r.err
	case r.row != nil: // first row gotten by an interator will be handled this way
		r.handleRow(dest)
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
	r.done = true
	if r.iter != nil {
		r.iter.Stop()
	}
	return nil
}
