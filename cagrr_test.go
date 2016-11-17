package cagrr_test

import (
	. "github.com/skbkontur/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Ring", func() {
	var (
		r Config
	)

	BeforeEach(func() {
		r = Config{
			Host: "localhost",
			Port: 8080,
		}
	})

	It("can be created from JSON", func() {
		Expect(r).To(MatchFields(IgnoreExtras,
			Fields{
				"Host": Equal("localhost"),
				"Port": BeNumerically("==", 8080),
			}))
	})
})
