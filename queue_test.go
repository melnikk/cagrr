package cagrr_test

import (
	"time"

	. "github.com/skbkontur/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Queue", func() {

	Context("One queue", func() {
		var queue DurationQueue
		BeforeEach(func() {
			queue = NewQueue(5)
		})

		It("should return 1s value when empty", func() {
			Expect(queue.Average()).To(Equal(time.Second))
		})

		It("should return average value with only one element", func() {
			queue.Push(5)
			Expect(queue.Average()).To(Equal(time.Duration(1)))
		})

		Context("full queue", func() {
			BeforeEach(func() {
				queue.Push(1)
				queue.Push(2)
				queue.Push(3)
				queue.Push(4)
				queue.Push(5)
			})
			It("should return average when full", func() {
				Expect(queue.Average()).To(Equal(time.Duration(3)))
			})
			It("should shift when overpush", func() {
				queue.Push(6)
				Expect(queue.Average()).To(Equal(time.Duration(4)))
			})
			It("should preserve length when overpush", func() {
				queue.Push(6)
				Expect(queue.Len()).To(Equal(5))
			})
			It("should stringify node value", func() {
				node := queue.Pop()
				Expect(node.String()).To(Equal("1ns"))
			})

			It("should shift from beginning when pop", func() {
				n := queue.Pop()
				Expect(n).To(Equal(time.Duration(1)))
			})

			It("should truncate queue when pop", func() {
				_ = queue.Pop()
				Expect(queue.Len()).To(Equal(4))
			})

			It("should truncate element only when oversized", func() {
				_ = queue.Pop()
				queue.Push(6)
				Expect(queue.Average()).To(Equal(time.Duration(4)))
			})
		})
	})
	Context("Multiple queues", func() {
		var queue1 DurationQueue
		var queue2 DurationQueue
		var queue3 DurationQueue
		BeforeEach(func() {
			queue1 = NewQueue(5)
			queue2 = NewQueue(5)
			queue3 = NewQueue(5)
		})

		It("should return average when all queues are full", func() {
			queue1.Push(1)
			queue1.Push(2)
			queue1.Push(3)
			queue1.Push(4)
			queue1.Push(5)

			queue2.Push(10)
			queue2.Push(20)
			queue2.Push(30)
			queue2.Push(40)
			queue2.Push(50)

			queue3.Push(100)
			queue3.Push(200)
			queue3.Push(300)
			queue3.Push(400)
			queue3.Push(500)

			Expect(queue1.Average()).To(Equal(time.Duration(3)))
			Expect(queue2.Average()).To(Equal(time.Duration(30)))
			Expect(queue3.Average()).To(Equal(time.Duration(300)))
		})
	})
})
