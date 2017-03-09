package sqlspanner

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"cloud.google.com/go/spanner"
)

type drv struct{}

func init() {
	sql.Register("spanner", &drv{})
}

func (d *drv) Open(name string) (driver.Conn, error) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, name)
	if err != nil {
		return nil, err
	}
	return &conn{
		ctx:    ctx,
		client: client,
	}, nil
}
