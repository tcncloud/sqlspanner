package sqlspanner_test

import (
	"fmt"

	// . "github.com/tcncloud/sqlspanner"
	"github.com/xwb1989/sqlparser"

	. "github.com/onsi/ginkgo"
	// . "github.com/onsi/gomega"
)

var _ = Describe("Conn", func() {
	Describe("Parse()", func() {
		Describe("given a select statement", func() {
			It("should parse into *sqlparser.Select", func() {
				stmt, _ := sqlparser.Parse("SELECT * FROM test")
				switch stmt.(type) {
				case *sqlparser.Select:
					fmt.Print("Select stmt")
				}
			})
		})

	})
})
