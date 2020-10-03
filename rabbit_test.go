// NOTE: These tests require RabbitMQ to be available on "amqp://localhost"
//
// Make sure that you do `docker-compose up` before running tests
package rabbit

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Rabbit", func() {
	var ()

	BeforeEach(func() {})

	Describe("New", func() {
		Context("when instantiating rabbit", func() {
			It("happy: should return a rabbit instance", func() {
				opts := generateOptions()

				r, err := New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())
			})

			It("should error with bad options", func() {
				r, err := New(nil)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("cannot be nil"))
				Expect(r).To(BeNil())
			})

			It("should error with unreachable rabbit server", func() {
				opts := generateOptions()
				opts.URL = "amqp://bad-url"

				r, err := New(opts)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("no such host"))
				Expect(r).To(BeNil())
			})
		})
	})
})

func generateOptions() *Options {
	return &Options{
		URL:               "amqp://localhost",
		QueueName:         "rabbit-test",
		ExchangeName:      "rabbit",
		ExchangeType:      "topic",
		RoutingKey:        "messages",
		QosPrefetchCount:  0,
		QosPrefetchSize:   0,
		RetryReconnectSec: 10,
		QueueDurable:      false,
		QueueExclusive:    true,
		QueueAutoDelete:   true,
	}
}
