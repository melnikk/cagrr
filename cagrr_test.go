package cagrr_test

import (
	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/reporters"
	. "github.com/onsi/gomega"
	"github.com/skbkontur/cagrr"

	"testing"
)

func TestCagrr(t *testing.T) {
	RegisterFailHandler(Fail)
	junitReporter := reporters.NewJUnitReporter("junit.xml")
	RunSpecsWithDefaultAndCustomReporters(t, "Cagrr Suite", []Reporter{junitReporter})
}

var _ = Describe("Cagrr", func() {
	var (
		emptyRing cagrr.Ring
	)

	BeforeEach(func() {
		emptyRing = cagrr.Ring{}
	})
	Describe("Ring", func() {
		It("should be a empty", func() {
			Expect(emptyRing.Count()).To(Equal(0))
		})
	})
})
