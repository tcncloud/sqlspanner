package sqlspanner

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/Sirupsen/logrus"

	"cloud.google.com/go/spanner"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

type drv struct{}

func init() {
	sql.Register("spanner", &drv{})
}

func (d *drv) Open(name string) (driver.Conn, error) {
	logrus.WithField("spanner db path", name).Debug("database connection")
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
