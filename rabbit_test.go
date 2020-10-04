// NOTE: These tests require RabbitMQ to be available on "amqp://localhost"
//
// Make sure that you do `docker-compose up` before running tests
package rabbit

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var _ = Describe("Rabbit", func() {
	Describe("New", func() {
		When("instantiating rabbit", func() {
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
				Expect(err.Error()).To(ContainSubstring("unable to dial server"))
				Expect(r).To(BeNil())
			})

			It("instantiates various internals", func() {
				opts := generateOptions()

				r, err := New(opts)

				Expect(err).To(BeNil())
				Expect(r).ToNot(BeNil())

				Expect(r.ctx).ToNot(BeNil())
				Expect(r.cancel).ToNot(BeNil())
				Expect(r.Conn).ToNot(BeNil())
				Expect(r.ConsumerRWMutex).ToNot(BeNil())
				Expect(r.NotifyCloseChan).ToNot(BeNil())
				Expect(r.ProducerRWMutex).ToNot(BeNil())
				Expect(r.ConsumeLooper).ToNot(BeNil())
				Expect(r.Options).ToNot(BeNil())
			})

			It("launches NotifyCloseChan watcher", func() {
				opts := generateOptions()

				r, err := New(opts)

				Expect(err).To(BeNil())
				Expect(r).ToNot(BeNil())

				// Before we write errors to the notify channel, copy previous
				// conn and channels so we can compare them after reconnect
				oldConn := r.Conn
				oldNotifyCloseChan := r.NotifyCloseChan
				oldConsumerDeliveryChannel := r.ConsumerDeliveryChannel

				// Write an error to the NotifyCloseChan
				r.NotifyCloseChan <- &amqp.Error{
					Code:    0,
					Reason:  "Test failure",
					Server:  false,
					Recover: false,
				}

				// Give our watcher a moment to see the msg and cause a reconnect
				time.Sleep(100 * time.Millisecond)

				// We should've reconnected and got a new conn
				Expect(r.Conn).ToNot(BeNil())
				Expect(r.Conn).To(BeAssignableToTypeOf(&amqp.Connection{}))
				Expect(oldConn).ToNot(Equal(r.Conn))

				// We should also get new channels
				Expect(r.NotifyCloseChan).ToNot(BeNil())
				Expect(r.ConsumerDeliveryChannel).ToNot(BeNil())
				Expect(oldNotifyCloseChan).ToNot(Equal(r.NotifyCloseChan))
				Expect(oldConsumerDeliveryChannel).ToNot(Equal(r.ConsumerDeliveryChannel))
			})
		})
	})

	Describe("Consume", func() {
		var (
			opts *Options
			r    *Rabbit

			errChan = make(chan *ConsumeError, 1)
			ch      *amqp.Channel
		)

		When("consuming messages with a context", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("run function is executed with inbound message", func() {
				receivedMessages := make([]amqp.Delivery, 0)

				// Launch consumer
				go func() {
					r.Consume(context.Background(), errChan, func(msg amqp.Delivery) error {
						receivedMessages = append(receivedMessages, msg)
						return nil
					})
				}()

				messages := generateRandomStrings(10)

				// Publish messages
				publishErr := publishMessages(ch, opts, messages)
				Expect(publishErr).To(BeNil())

				// Wait
				time.Sleep(100 * time.Millisecond)

				// Verify messages we received
				stopErr := r.Stop()
				Expect(stopErr).ToNot(HaveOccurred())

				var data []string

				// Verify message attributes
				for _, msg := range receivedMessages {
					Expect(msg.Exchange).To(Equal(opts.ExchangeName))
					Expect(msg.RoutingKey).To(Equal(opts.RoutingKey))

					data = append(data, string(msg.Body))
				}

				Expect(messages).To(Equal(data))
			})

			It("context can be used to cancel consume", func() {
				ctx, cancel := context.WithCancel(context.Background())
				receivedMessages := make([]amqp.Delivery, 0)

				var exit bool

				// Launch consumer
				go func() {
					r.Consume(ctx, errChan, func(msg amqp.Delivery) error {
						receivedMessages = append(receivedMessages, msg)
						return nil
					})

					exit = true
				}()

				messages := generateRandomStrings(20)

				// Publish 5 messages -> cancel -> publish remainder of messages ->
				// verify runfunc was hit only 5 times
				publishErr1 := publishMessages(ch, opts, messages[0:10])
				Expect(publishErr1).ToNot(HaveOccurred())

				// Wait a moment for consumer to pick up messages
				time.Sleep(100 * time.Millisecond)

				cancel()

				// Wait a moment for consumer to quit
				time.Sleep(100 * time.Millisecond)

				publishErr2 := publishMessages(ch, opts, messages[10:])
				Expect(publishErr2).ToNot(HaveOccurred())

				Expect(len(receivedMessages)).To(Equal(10))
				Expect(exit).To(BeTrue())
			})
		})

		When("consuming messages with an error channel", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("any errors returned by run func are passed to error channel", func() {
				go func() {
					r.Consume(context.Background(), errChan, func(msg amqp.Delivery) error {
						return errors.New("stuff broke")
					})
				}()

				messages := generateRandomStrings(1)

				publishErr := publishMessages(ch, opts, messages)
				Expect(publishErr).ToNot(HaveOccurred())

				Eventually(func() string {
					consumeErr := <-errChan
					return consumeErr.Error.Error()
				}).Should(ContainSubstring("stuff broke"))
			})
		})

		When("when a nil error channel is passed in", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("errors are discarded and Consume() continues to work", func() {
				receivedMessages := make([]string, 0)

				go func() {
					r.Consume(context.Background(), nil, func(msg amqp.Delivery) error {
						receivedMessages = append(receivedMessages, string(msg.Body))
						return errors.New("stuff broke")
					})
				}()

				// Publish a handful of messages
				messages := generateRandomStrings(10)

				publishErr := publishMessages(ch, opts, messages)
				Expect(publishErr).To(BeNil())

				// Wait
				time.Sleep(100 * time.Millisecond)

				// Verify messages we received
				stopErr := r.Stop()
				Expect(stopErr).ToNot(HaveOccurred())

				// Verify message attributes
				Expect(messages).To(Equal(receivedMessages))
			})
		})

		When("a nil context is passed in", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("Consume() continues to work", func() {
				receivedMessages := make([]string, 0)

				go func() {
					r.Consume(nil, nil, func(msg amqp.Delivery) error {
						receivedMessages = append(receivedMessages, string(msg.Body))
						return errors.New("stuff broke")
					})
				}()

				// Publish a handful of messages
				messages := generateRandomStrings(10)

				publishErr := publishMessages(ch, opts, messages)
				Expect(publishErr).To(BeNil())

				// Wait
				time.Sleep(100 * time.Millisecond)

				// Verify messages we received
				stopErr := r.Stop()
				Expect(stopErr).ToNot(HaveOccurred())

				// Verify message attributes
				Expect(messages).To(Equal(receivedMessages))
			})
		})
	})

	Describe("ConsumeOnce", func() {
		var (
			opts *Options
			r    *Rabbit

			ch *amqp.Channel
		)

		When("passed context is nil", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("will continue to work", func() {
				var receivedMessage string
				var consumeErr error
				var exit bool

				go func() {
					consumeErr = r.ConsumeOnce(nil, func(msg amqp.Delivery) error {
						receivedMessage = string(msg.Body)
						return nil
					})

					exit = true
				}()

				// Wait a moment for consumer to start
				time.Sleep(100 * time.Millisecond)

				// Generate a handful of messages
				messages := generateRandomStrings(10)

				publishErr := publishMessages(ch, opts, messages)

				Expect(publishErr).ToNot(HaveOccurred())

				// Wait a moment for consumer to get the message
				time.Sleep(100 * time.Millisecond)

				Expect(consumeErr).ToNot(HaveOccurred())

				// Received message should be the same as the first message
				Expect(receivedMessage).To(Equal(messages[0]))

				// Goroutine should've exited
				Expect(exit).To(BeTrue())
			})
		})

		When("context is passed", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("will listen for cancellation", func() {
				var consumeErr error
				var exit bool
				var receivedMessage string

				ctx, cancel := context.WithCancel(context.Background())

				go func() {
					consumeErr = r.ConsumeOnce(ctx, func(msg amqp.Delivery) error {
						receivedMessage = string(msg.Body)
						return nil
					})

					exit = true
				}()

				// Wait a moment for consumer to connect
				time.Sleep(100 * time.Millisecond)

				// Consumer should not have received a message or exited
				Expect(receivedMessage).To(BeEmpty())
				Expect(exit).To(BeFalse())

				cancel()

				// Wait for cancel to kick in
				time.Sleep(100 * time.Millisecond)

				// Goroutine should've exited
				Expect(exit).To(BeTrue())
				Expect(consumeErr).ToNot(HaveOccurred())
			})
		})

		When("run func gets an error", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("will return the error to the user", func() {
				var consumeErr error
				var exit bool

				go func() {
					consumeErr = r.ConsumeOnce(nil, func(msg amqp.Delivery) error {
						return errors.New("something broke")
					})

					exit = true
				}()

				// Wait a moment for consumer to connect
				time.Sleep(100 * time.Millisecond)

				// Generate and send a message
				messages := generateRandomStrings(1)
				publishErr := publishMessages(ch, opts, messages)

				Expect(publishErr).ToNot(HaveOccurred())

				// Wait a moment for consumer to receive the message
				time.Sleep(100 * time.Millisecond)

				// Goroutine should've exited
				Expect(exit).To(BeTrue())

				// Consumer should've returned correct error
				Expect(consumeErr).To(HaveOccurred())
				Expect(consumeErr.Error()).To(ContainSubstring("something broke"))

			})
		})
	})

	Describe("Publish", func() {
		var (
			opts *Options
			r    *Rabbit
			ch   *amqp.Channel
		)

		Context("happy path", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("correctly publishes message", func() {
				var receivedMessage []byte

				go func() {
					var err error
					receivedMessage, err = receiveMessage(ch, opts)

					Expect(err).ToNot(HaveOccurred())
				}()

				testMessage := []byte(uuid.NewV4().String())
				publishErr := r.Publish(nil, opts.RoutingKey, testMessage)

				Expect(publishErr).ToNot(HaveOccurred())

				// Give our consumer some time to receive the message
				time.Sleep(100 * time.Millisecond)

				Expect(receivedMessage).To(Equal(testMessage))
			})
		})

		When("producer server channel is nil", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("will generate a new server channel", func() {
				r.ProducerServerChannel = nil

				var receivedMessage []byte

				go func() {
					var err error
					receivedMessage, err = receiveMessage(ch, opts)

					Expect(err).ToNot(HaveOccurred())
				}()

				testMessage := []byte(uuid.NewV4().String())
				publishErr := r.Publish(nil, opts.RoutingKey, testMessage)

				Expect(publishErr).ToNot(HaveOccurred())

				// Give our consumer some time to receive the message
				time.Sleep(100 * time.Millisecond)

				Expect(receivedMessage).To(Equal(testMessage))
			})
		})
	})

	Describe("Stop", func() {
		var (
			opts *Options
			r    *Rabbit
			ch   *amqp.Channel
		)

		When("consuming messages via Consume()", func() {
			BeforeEach(func() {
				var err error

				opts = generateOptions()

				r, err = New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())

				ch, err = connect(opts)
				Expect(err).ToNot(HaveOccurred())
				Expect(ch).ToNot(BeNil())
			})

			It("Stop() should release Consume() and return", func() {
				var receivedMessage string
				var exit bool

				go func() {
					r.Consume(nil, nil, func(msg amqp.Delivery) error {
						receivedMessage = string(msg.Body)
						return nil
					})

					exit = true
				}()

				// Wait a moment for consumer to start
				time.Sleep(100 * time.Millisecond)

				// Stop the consumer
				stopErr := r.Stop()

				time.Sleep(100 * time.Millisecond)

				// Verify that stop did not error and the goroutine exited
				Expect(stopErr).ToNot(HaveOccurred())
				Expect(receivedMessage).To(BeEmpty()) // jic
				Expect(exit).To(BeTrue())
			})
		})
	})

	Describe("validateOptions", func() {
		var (
			opts *Options
		)

		Context("validation combinations", func() {
			BeforeEach(func() {
				opts = generateOptions()
			})

			It("errors with nil options", func() {
				err := ValidateOptions(nil)
				Expect(err).To(HaveOccurred())
			})

			It("errors when URL is unset", func() {
				opts.URL = ""

				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("URL cannot be empty"))
			})

			It("only checks ExchangeType if ExchangeDeclare is true", func() {
				opts.ExchangeDeclare = false
				opts.ExchangeType = ""

				err := ValidateOptions(opts)
				Expect(err).ToNot(HaveOccurred())

				opts.ExchangeDeclare = true
				opts.ExchangeType = ""

				err = ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ExchangeType cannot be empty"))
			})

			It("errors if ExchangeName is unset", func() {
				opts.ExchangeName = ""

				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ExchangeName cannot be empty"))
			})

			It("errors if RoutingKey is unset", func() {
				opts.RoutingKey = ""

				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("RoutingKey cannot be empty"))
			})

			It("sets RetryConnect to default if unset", func() {
				opts.RetryReconnectSec = 0

				err := ValidateOptions(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(opts.RetryReconnectSec).To(Equal(DefaultRetryReconnectSec))
			})
		})
	})
})

func generateOptions() *Options {
	exchangeName := "rabbit-" + uuid.NewV4().String()

	return &Options{
		URL:                "amqp://localhost",
		QueueName:          "rabbit-" + uuid.NewV4().String(),
		ExchangeName:       exchangeName,
		ExchangeType:       "topic",
		ExchangeDeclare:    true,
		ExchangeDurable:    false,
		ExchangeAutoDelete: true,
		RoutingKey:         exchangeName,
		QosPrefetchCount:   0,
		QosPrefetchSize:    0,
		RetryReconnectSec:  10,
		QueueDeclare:       true,
		QueueDurable:       false,
		QueueExclusive:     false,
		QueueAutoDelete:    true,
	}
}

func generateRandomStrings(num int) []string {
	generated := make([]string, 0)

	for i := 0; i != num; i++ {
		generated = append(generated, uuid.NewV4().String())
	}

	return generated
}

func connect(opts *Options) (*amqp.Channel, error) {
	ac, err := amqp.Dial(opts.URL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to dial rabbit server")
	}

	ch, err := ac.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

	return ch, nil
}

func publishMessages(ch *amqp.Channel, opts *Options, messages []string) error {
	for _, v := range messages {
		if err := ch.Publish(opts.ExchangeName, opts.RoutingKey, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(v),
		}); err != nil {
			return err
		}
	}

	return nil
}

func receiveMessage(ch *amqp.Channel, opts *Options) ([]byte, error) {
	tmpQueueName := "rabbit-receiveMessages-" + uuid.NewV4().String()

	if _, err := ch.QueueDeclare(
		tmpQueueName,
		false,
		true,
		false,
		false,
		nil,
	); err != nil {
		return nil, errors.Wrap(err, "unable to declare queue")
	}

	if err := ch.QueueBind(tmpQueueName, opts.RoutingKey, opts.ExchangeName, false, nil); err != nil {
		return nil, errors.Wrap(err, "unable to bind queue")
	}

	deliveryChan, err := ch.Consume(tmpQueueName, "", true, false, false, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create delivery channel")
	}

	select {
	case m := <-deliveryChan:
		logrus.Debug("Test: received message in receiveMessage()")
		return m.Body, nil
	case <-time.After(5 * time.Second):
		logrus.Debug("Test: timed out waiting for message in receiveMessage()")
		return nil, errors.New("timed out")
	}
}
