package cagrr_test

import (
	. "github.com/skbkontur/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	Context("Structure", func() {
		It("Should read configuration file", func() {
			config, err := ReadConfig("pkg/config.yml")

			Expect(len(config.Clusters)).To(Equal(2))
			Expect(err).To(BeNil())
		})

		It("Should return error when file is absent", func() {
			_, err := ReadConfig("pkg/noconfig.yml")

			Expect(err).NotTo(BeNil())
		})

		It("Should read deep structure of Keyspaces", func() {
			/*config, _ := ReadConfig("pkg/config.yml")

			Expect(config.Clusters[0]).To(BeNil())*/
		})

	})
})
