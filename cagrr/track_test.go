package cagrr_test

import (
	"time"

	. "github.com/skbkontur/cagrr/cagrr"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Track", func() {
	var track *Track
	BeforeEach(func() {
		track = &Track{}
	})

	It("should be new", func() {
		Expect(track.IsNew()).To(BeTrue())
	})

	Context("started repairs", func() {
		BeforeEach(func() {
			track.Start(5)
		})

		It("shouldn't be new", func() {
			Expect(track.IsNew()).To(BeFalse())
		})

		Context("completion", func() {
			It("should increment percent", func() {
				err := false
				track.Complete(time.Duration(0), err)
				Expect(track.Percent).To(BeNumerically("==", 20))
				track.Complete(time.Duration(0), err)
				Expect(track.Percent).To(BeNumerically("==", 40))
				track.Complete(time.Duration(0), err)
				Expect(track.Percent).To(BeNumerically("==", 60))
				track.Complete(time.Duration(0), err)
				Expect(track.Percent).To(BeNumerically("==", 80))
				track.Complete(time.Duration(0), err)
				Expect(track.Percent).To(BeNumerically("==", 100))
			})
		})
		Context("skipping", func() {
			It("should increment percent", func() {
				track.Skip()
				Expect(track.Percent).To(BeNumerically("==", 20))
				track.Skip()
				Expect(track.Percent).To(BeNumerically("==", 40))
				track.Skip()
				Expect(track.Percent).To(BeNumerically("==", 60))
				track.Skip()
				Expect(track.Percent).To(BeNumerically("==", 80))
				track.Skip()
				Expect(track.Percent).To(BeNumerically("==", 100))
			})
		})
		Context("completed", func() {
			BeforeEach(func() {
				err := false
				track.Start(5)
				track.Complete(time.Duration(0), err)
				track.Complete(time.Duration(0), err)
				track.Complete(time.Duration(0), err)
				track.Complete(time.Duration(0), err)
				track.Complete(time.Duration(0), err)
			})
			It("should be repaired", func() {
				isRepaired := track.IsRepaired(time.Second * 1000)
				Expect(isRepaired).To(BeTrue())
			})

			It("should restart", func() {
				track.Restart()
				isRepaired := track.IsRepaired(time.Second * 1000)
				Expect(isRepaired).To(BeFalse())
			})

		})

	})
})
