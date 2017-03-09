package sqlspanner

import "fmt"

type result struct {
	lastId       *int64
	rowsAffected *int64
}

func (r *result) LastInsertId() (int64, error) {
	if r.lastId != nil {
		return *r.lastId, nil
	} else {
		return 0, fmt.Errorf("no last inserted id set")
	}
}

func (r *result) RowsAffected() (int64, error) {
	if r.rowsAffected != nil {
		return *r.rowsAffected, nil
	} else {
		return 0, fmt.Errorf("no rows affected set")
	}
}
