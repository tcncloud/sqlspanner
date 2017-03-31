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
	_ "github.com/tcncloud/sqlspanner"

	"database/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Conn", func() {
	Describe("given a db connection ", func() {
		conn, err := sql.Open("spanner", "projects/algebraic-ratio-149721/instances/test-instance/databases/test-project")
		It("should connect succesfuly", func() {
			Expect(err).To(BeNil())
		})
		It("should be able to ping", func() {
			Expect(conn.Ping()).To(BeNil())
		})
		It("should be able to execute an insert statement", func() {
			_, err := conn.Exec("INSERT INTO test_table1(id, simple_string) VALUES(?, ?)", 1, "test_string")
			Expect(err).To(BeNil())
		})
		It("should be able to execute an update statement", func() {
			_, err = conn.Exec(`UPDATE test_table1 SET simple_string=? WHERE id=1`, "changed test string")
		})
		It("should be able to execute a delete statement", func() {
			_, err = conn.Exec("DELETE FROM test_table1 WHERE id = 1 ", nil)
			Expect(err).To(BeNil())
		})
	})
})
