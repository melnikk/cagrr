package cagrr_test

import (
	. "github.com/skbkontur/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cagrr", func() {
	var (
		emptyRing Ring
	)

	BeforeEach(func() {
		emptyRing = Ring{}
	})

	It("should be a empty", func() {
		Expect(emptyRing.Count()).To(Equal(0))
	})
})
