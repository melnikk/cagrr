package cagrr_test

import (
	"time"

	. "github.com/skbkontur/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Regulator", func() {
	var regulator Regulator
	BeforeEach(func() {
		regulator = NewRegulator(5)
	})

	Context("Single cluster", func() {
		var key string
		BeforeEach(func() {
			key = "queue"
		})
		It("should return 1s value when no queue", func() {
			Expect(regulator.Rate(key)).To(Equal(time.Second))
		})

		It("should average limits", func() {
			regulator.LimitRateTo(key, 5)
			Expect(regulator.Rate(key)).To(Equal(time.Duration(1)))
		})

		Measure("should sleep to average rate", func(b Benchmarker) {

			runtime := b.Time("runtime", func() {
				key := "queue"
				regulator.LimitRateTo(key, time.Millisecond*100)
				regulator.LimitRateTo(key, time.Millisecond*200)
				regulator.LimitRateTo(key, time.Millisecond*300)
				regulator.LimitRateTo(key, time.Millisecond*400)
				regulator.LimitRateTo(key, time.Millisecond*500)
				regulator.Limit(key)
			})

			Expect(runtime.Seconds()).Should(BeNumerically("~", 0.3, 0.01))
		}, 1)
	})

	Context("Multiple clusters", func() {
		var key1, key2, key3 string
		BeforeEach(func() {
			key1 = "key1"
			key2 = "key2"
			key3 = "key3"
		})
		It("shouldn't affect each other", func() {
			regulator.LimitRateTo(key1, 1)
			regulator.LimitRateTo(key1, 2)
			regulator.LimitRateTo(key1, 3)
			regulator.LimitRateTo(key1, 4)
			regulator.LimitRateTo(key1, 5)

			regulator.LimitRateTo(key2, 10)
			regulator.LimitRateTo(key2, 20)
			regulator.LimitRateTo(key2, 30)
			regulator.LimitRateTo(key2, 40)
			regulator.LimitRateTo(key2, 50)

			regulator.LimitRateTo(key3, 100)
			regulator.LimitRateTo(key3, 200)
			regulator.LimitRateTo(key3, 300)
			regulator.LimitRateTo(key3, 400)
			regulator.LimitRateTo(key3, 500)

			Expect(regulator.Rate(key1)).To(Equal(time.Duration(3)))
			Expect(regulator.Rate(key2)).To(Equal(time.Duration(30)))
			Expect(regulator.Rate(key3)).To(Equal(time.Duration(300)))
		})
	})
})
