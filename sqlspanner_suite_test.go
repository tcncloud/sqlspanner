package sqlspanner_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestSqlspanner(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sqlspanner Suite")
}
