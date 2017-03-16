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
		It("should be able to execute a delete statment", func() {
			_, err := conn.Exec("DELETE FROM test_table WHERE id=1 AND simple_string=?", "test_string")
			Expect(err).To(BeNil())
		})
	})
})
