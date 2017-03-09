package sqlspanner

import "fmt"

type result struct {
	lastID       *int64
	rowsAffected *int64
}

func (r *result) LastInsertId() (int64, error) {
	if r.lastID != nil {
		return *r.lastID, nil
	}
	return 0, fmt.Errorf("no last inserted id set")
}

func (r *result) RowsAffected() (int64, error) {
	if r.rowsAffected != nil {
		return *r.rowsAffected, nil
	}
	return 0, fmt.Errorf("no rows affected set")
}
