package sqlspanner_test

import (
	"database/sql/driver"
	"io"
	//"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/tcncloud/sqlspanner"
	//"cloud.google.com/go/spanner"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

var _ = Describe("Rows", func() {
	Describe("New Rows", func() {
		Describe("with a valid row iterator", func() {
			// grab spanner's row iterator
			It("converts iterator to nextable iterface", func() {
			})
		})
		Describe("with nextable interface", func() {
			Describe("with empty iterator", func() {
				next := sqlspanner.NewTestNextable(0, "values")
				rows := sqlspanner.NewRowsFromNextable(next)
				It("does not have columns ", func() {
					cols := rows.Columns()
					Expect(cols).To(BeZero())
				})

				It("sets err field to io.EOF", func() {
					row := make([]driver.Value, 0)
					err := rows.Next(row)
					Expect(row).To(BeEmpty())
					Expect(err).To(BeEquivalentTo(io.EOF))
				})
			})
			Describe("with full iterator", func() {
				Describe(`with values that are types:
						string, int64, float64, []byte, bool, []bool, []int64, []float64, []string, [][]byte`, func() {
					next := sqlspanner.NewTestNextable(2, "values")
					rows := sqlspanner.NewRowsFromNextable(next)
					It("has columns", func() {
						Expect(rows.Columns()).To(BeEquivalentTo([]string{
							"a", "b", "c", "d", "e",
							"f", "g", "h", "i", "j",
						}))
					})

					It("gets correct []driver.Value for number of rows that are in iterator", func() {
						for i := 0; i < 2; i++ {
							row := make([]driver.Value, 10)
							err := rows.Next(row)
							Expect(err).To(BeZero())
							//fmt.Printf("row: %#v, \n\n%+v\n\n", row, row)
							Expect(row).To(BeEquivalentTo(next.WhatValueRowShouldBe()))
						}
						row := make([]driver.Value, 10)
						err := rows.Next(row)
						Expect(err).To(BeEquivalentTo(io.EOF))
					})
				})

				Describe(`with values that are types: civil.Date, time.Time, []civil.Date, []time.Time`, func() {
					next := sqlspanner.NewTestNextable(2, "times")
					rows := sqlspanner.NewRowsFromNextable(next)
					It("gets correct []driver.Value for number of rows that are in iterator", func() {
						for i := 0; i < 2; i++ {
							row := make([]driver.Value, 4)
							err := rows.Next(row)
							Expect(err).To(BeZero())
							//fmt.Printf("row: %#v, \n\n%+v\n\n", row, row)
							Expect(row).To(BeEquivalentTo(next.WhatTimeRowShouldBe()))
						}
						row := make([]driver.Value, 4)
						err := rows.Next(row)
						Expect(err).To(BeEquivalentTo(io.EOF))
					})
				})
			})
		})
		Describe("with spanner.Row", func() {
			Describe("with populated row", func() {
				It("sets cols field", func() {

				})

				It("sets row field", func() {

				})

				It("does not set err field", func() {

				})

				It("sets valuer field", func() {

				})
			})

			Describe("with nil row", func() {
				It("does not set cols field", func() {

				})

				It("does not set row field", func() {

				})

				It("sets err field to io.EOF", func() {

				})
			})
		})

	})
})
