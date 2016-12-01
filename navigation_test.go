package cagrr_test

import (
	. "github.com/skbkontur/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Navigation", func() {
	nav := Navigation{"cluster", "keyspace", "table"}

	It("Should pass when all field are same", func() {
		Expect(nav.Is("cluster", "keyspace", "table")).To(BeTrue())
	})
})
