package cagrr_test

import (
	. "github.com/skbkontur/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Config", func() {
	Context("File exist", func() {
		config, err := ReadConfiguration("pkg/config.yml")
		It("Should not return error", func() {
			Expect(err).To(BeNil())
		})
		It("Should contain cajrr connection params", func() {
			Expect(config.Conn).To(MatchFields(IgnoreExtras,
				Fields{
					"Host": Equal("localhost"),
					"Port": BeNumerically("==", 8080),
				}))
		})
	})
	Context("File didn't exist", func() {
		config, err := ReadConfiguration("pkg/unexisted.yml")
		It("Should return error", func() {
			Expect(err).NotTo(BeNil())
		})
		It("Should return no config", func() {
			Expect(config).To(BeNil())
		})
	})
})
