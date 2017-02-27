package cagrr_test

import (
	. "github.com/skbkontur/cagrr/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Table", func() {
	var table *Table
	BeforeEach(func() {
		table = &Table{}
	})
	Context("totals", func() {
		It("should be empty", func() {
			Expect(table.Total()).To(Equal(0))
		})

		It("should set total", func() {
			table.SetTotal(5)
			Expect(table.Total()).To(Equal(5))
		})

	})
	Context("repairs", func() {
		It("should be nil", func() {
			Expect(table.Repairs()).To(BeNil())
		})

		It("should set repairs", func() {
			repair := &Repair{}
			repairs := []*Repair{}
			repairs = append(repairs, repair)
			table.SetRepairs(repairs)

			Expect(table.Repairs()[0]).To(BeIdenticalTo(repair))
		})

	})
})
