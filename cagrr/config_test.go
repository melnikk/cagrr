package cagrr_test

import (
	. "github.com/skbkontur/cagrr/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	Context("File exist", func() {
		_, err := ReadConfiguration("../pkg/config.yml")
		It("Should not return error", func() {
			Expect(err).To(BeNil())
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
