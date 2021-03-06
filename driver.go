//
// Copyright 2017, TCN Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of TCN Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package sqlspanner

import (
	"time"
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/Sirupsen/logrus"
	"cloud.google.com/go/civil"
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

func IsValue(v interface{}) bool {
	switch v.(type){
	case int, int64, spanner.NullInt64:
		return true
	case string, spanner.NullString:
		return true
	case []string, []spanner.NullString:
		return true
	case []int, []int64, []spanner.NullInt64:
		return true
	case bool, spanner.NullBool:
		return true
	case float64, spanner.NullFloat64:
		return true
	case []float64, []spanner.NullFloat64:
		return true
	case time.Time, spanner.NullTime:
		return true
	case []time.Time, []spanner.NullTime:
		return true
	case civil.Date, []civil.Date:
		return true
	case nil:
		return true
	}
	return false
}

func needsEncoding(v interface{}) bool {
	switch v.(type) {
	case int, []string, []int, []int64, []bool, []float64, []time.Time:
		return true
	case civil.Date, spanner.NullInt64, spanner.NullString, spanner.NullFloat64:
		return true
	case []civil.Date, []spanner.NullTime, []spanner.NullInt64, []spanner.NullString:
		return true
	case []spanner.NullFloat64, []spanner.NullBool:
		return true
	}
	return false
}

