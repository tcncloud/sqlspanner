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
	// . "github.com/tcncloud/sqlspanner"

	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stmt", func() {
	Describe("given a db connection", func() {
		conn, err := sql.Open("spanner", spannerTestDatabase)

		It("should connect successfully", func() {
			Expect(err).To(BeNil())
		})
		It("can be used multiple times with different parameters", func() {
			stmt, err := conn.Prepare("INSERT INTO test_table1 (id, simple_string) VALUES(?, ?)")
			Expect(err).To(BeNil())

			_, err = stmt.Exec(1, "string1")
			Expect(err).To(BeNil())
			_, err = stmt.Exec(2, "string2")
			Expect(err).To(BeNil())
			err = stmt.Close()
			Expect(err).To(BeNil())

			stmt, err = conn.Prepare("SELECT id, simple_string FROM test_table1 WHERE id=? LIMIT 1")
			Expect(err).To(BeNil())

			var id int64
			var simple_string string

			row := stmt.QueryRow(1)
			err = row.Scan(&id, &simple_string)
			Expect(err).To(BeNil())
			Expect(id).To(Equal(int64(1)))
			Expect(simple_string).To(Equal("string1"))

			row = stmt.QueryRow(1)
			err = row.Scan(&id, &simple_string)
			Expect(err).To(BeNil())
			Expect(id).To(Equal(int64(2)))
			Expect(simple_string).To(Equal("string2"))
		})

		It("can delete using statement mutliple times", func() {
			stmt, err := conn.Prepare("DELETE FROM test_table1 WHERE id=?")
			Expect(err).To(BeNil())

			_, err = stmt.Exec(1)
			Expect(err).To(BeNil())
			_, err = stmt.Exec(2)
			Expect(err).To(BeNil())

			_, err = conn.Query("SELECT * FROM test_table1")
			Expect(err).To(Equal(sql.ErrNoRows))
		})
	})
})
