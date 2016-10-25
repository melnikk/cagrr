package cagrr_test

import (
	. "github.com/skbkontur/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Ring", func() {
	var (
		r Ring
	)

	BeforeEach(func() {
		r = Ring{
			Host: "localhost",
			Port: 8080,
		}
	})

	It("can be created from JSON", func() {
		Expect(r).To(MatchAllFields(
			Fields{
				"Host":        Equal("localhost"),
				"Port":        BeNumerically("==", 8080),
				"Cluster":     BeNumerically("==", 0),
				"Name":        Equal(""),
				"Partitioner": Equal(""),
				"Tokens":      Equal([]Token(nil)),
			}))
	})
})