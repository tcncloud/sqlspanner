package sqlspanner_test

import (
	//"database/sql"

	"github.com/Sirupsen/logrus"
	_ "github.com/tcncloud/sqlspanner"

	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

var _ = Describe("Rows", func() {
	Describe("with a valid row iterator", func() {
		// grab spanner's row iterator
		It("", func() {
		})
	})
})
