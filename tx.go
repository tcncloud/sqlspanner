package sqlspanner

type tx struct{}

func (t *tx) Commit() error {
	return unimplemented
}
func (t *tx) Rollback() error {
	return unimplemented
}
