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

package sqlspanner_test

import (
	"database/sql"

	"github.com/Sirupsen/logrus"
	_ "github.com/tcncloud/sqlspanner"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

var _ = Describe("Driver", func() {
	Describe("registering spanner driver", func() {
		It("should be found in the drivers list", func() {
			logrus.WithField("driver", sql.Drivers()).Info("Driver")
		})
	})
	Describe("given a valid db path", func() {
		Describe("connecting to db", func() {
			conn, err := sql.Open("spanner", "projects/algebraic-ratio-149721/instances/test-instance/databases/test-project")
			It("should not return an error", func() {
				Expect(err).To(BeNil())
			})
			It("should return a valid connection", func() {
				Expect(conn).ToNot(BeNil())
			})
			It("should ping the db", func() {
				Expect(conn.Ping()).To(BeNil())
			})
		})

	})
})
