package sqlspanner_test

import (
	"database/sql"

	// . "github.com/tcncloud/sqlspanner"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Driver", func() {
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
