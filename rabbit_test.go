package rabbit

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Rabbit", func() {
	var ()

	BeforeEach(func() {})

	Describe("New", func() {
		Context("when instantiating a rabbit instance", func() {
			It("should error when unable to connect to rabbit", func() {
				Expect(true).To(BeTrue())
			})
		})
	})
})
