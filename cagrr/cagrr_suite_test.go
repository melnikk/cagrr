package cagrr_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestCagrr(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cagrr Suite")
}
