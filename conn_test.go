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
	"cloud.google.com/go/spanner"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// Spanner costs money. So this database needs to be recreated every time we do testing
// these var says we have a project setup of:
// projectID: algebraic-ratio-149721
// instanceID: test-instance
// database: test-project
// table:  test_table1,
// Columns: (id (int64), simple_string (string)),
// Primary Key: id
//
// table: test_table2,
// Columns: (id (int64), id_string (string), simple_string(string), items (array<string>)
// Primary Key: id, id_string
var spannerTestDatabase = "projects/algebraic-ratio-149721/instances/test-instance/databases/test-project"

// TODO when selects get working,  actually test that the data gets inserted/updated/deleted
var _ = Describe("Conn", func() {
	Describe("given a db connection ", func() {
		conn, err := sql.Open("spanner", spannerTestDatabase)

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

		It("should be able to execute a select statement", func() {
			rows, err := conn.Query("SELECT * FROM test_table1")

			Expect(err).To(BeNil())
			Expect(rows.Columns()).To(BeEquivalentTo([]string{"id", "simple_string"}))
			hasNext := rows.Next()

			Expect(hasNext).To(Equal(true))
			Expect(rows.Err()).To(BeNil())
			var id int64
			var s string

			rows.Scan(&id, &s)
			Expect(id).To(Equal(int64(1)))
			Expect(s).To(Equal("test_string"))
			hasNext = rows.Next()
			Expect(hasNext).To(Equal(false))
			err = rows.Close()
			Expect(err).To(BeNil())
		})

		It("should be able to execute an update statement", func() {
			_, err = conn.Exec(`UPDATE test_table1 SET simple_string=? WHERE id=1`, "changed test string")
			Expect(err).To(BeNil())
		})

		It("should be able to execute a delete statement", func() {
			_, err = conn.Exec("DELETE FROM test_table1 WHERE id = 1 ", nil)
			Expect(err).To(BeNil())
		})

		It("should be able to insert more complex record, and retrieve it", func() {
			_, err := conn.Exec(`INSERT INTO test_table2(id, id_string, simple_string, items)
				VALUES(1, ?, "test_string", ?)`, "1", []string{"string1", "string2"})
			Expect(err).To(BeNil())
			row := conn.QueryRow(`SELECT id, id_string, items, simple_string FROM test_table2 LIMIT 1`)

			var id int64
			var id_string, simple_string string
			var items []spanner.NullString

			err = row.Scan(&id, &id_string, &items, &simple_string)
			Expect(err).To(BeNil())
			Expect(id).To(Equal(int64(1)))
			Expect(id_string).To(Equal("1"))
			Expect(simple_string).To(Equal("test_string"))
			Expect(items).To(BeEquivalentTo([]spanner.NullString{
				spanner.NullString{StringVal: "string1", Valid: true},
				spanner.NullString{StringVal: "string2", Valid: true},
			}))
		})

		It("should be able to update more complex record, and retrieve updated row", func() {
			arg := []spanner.NullString{
				spanner.NullString{StringVal: "changedstring1", Valid: true},
				spanner.NullString{StringVal: "", Valid: false},
			}
			_, err := conn.Exec(`UPDATE test_table2 SET items=? WHERE id = 1 AND id_string = "1"`, arg)
			Expect(err).To(BeNil())

			row := conn.QueryRow(`SELECT items FROM test_table2 WHERE id = 1 AND id_string = "1"`)

			var items []spanner.NullString

			err = row.Scan(&items)
			Expect(err).To(BeNil())
			Expect(items).To(BeEquivalentTo([]spanner.NullString{
				spanner.NullString{StringVal: "changedstring1", Valid: true},
				spanner.NullString{StringVal: "", Valid: false},
			}))
		})

		It("can delete", func() {
			_, err := conn.Exec(`DELETE FROM test_table2 WHERE id = 1 AND id_string = "1"`)
			Expect(err).To(BeNil())
		})

		It("can delete a range of things", func() {
			_, err := conn.Exec(`INSERT into test_table2 (id, id_string, simple_string)
				VALUES (1, "1", "test_string1")`)
			Expect(err).To(BeNil())
			_, err = conn.Exec(`INSERT into test_table2 (id, id_string, simple_string)
				VALUES (1, "2", "test_string2")`)
			Expect(err).To(BeNil())
			_, err = conn.Exec(`INSERT into test_table2 (id, id_string, simple_string)
				VALUES (1, "3", "test_string3")`)
			Expect(err).To(BeNil())
			_, err = conn.Exec(`DELETE FROM test_table2 WHERE id = 1 AND id_string >= 1 AND id_string <= 2`)
			Expect(err).To(BeNil())
			rows, err := conn.Query(`SELECT simple_string from test_table2`)
			Expect(err).To(BeNil())

			hasNext := rows.Next()

			Expect(hasNext).To(Equal(true))
			Expect(rows.Err()).To(BeNil())
			var simple_string string

			err = rows.Scan(&simple_string)
			Expect(err).To(BeNil())

			Expect(simple_string).To(Equal("test_string3"))
			hasNext = rows.Next()
			Expect(hasNext).To(Equal(false))
			_, err = conn.Exec(`DELETE FROM test_table2 WHERE id = 1 AND id_string = "3"`)
			Expect(err).To(BeNil())
		})
	})
})
