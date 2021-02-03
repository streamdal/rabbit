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
	var (
		opts *Options
		r    *Rabbit
		ch   *amqp.Channel
	)

	// This runs *after* all of the BeforeEach's AND *before* each It() block
	JustBeforeEach(func() {
		var err error

		opts = generateOptions()

		r, err = New(opts)

		Expect(err).ToNot(HaveOccurred())
		Expect(r).ToNot(BeNil())

		ch, err = connect(opts)
		Expect(err).ToNot(HaveOccurred())
		Expect(ch).ToNot(BeNil())
	})

	Describe("New", func() {
		When("instantiating rabbit", func() {
			It("happy: should return a rabbit instance", func() {
				opts := generateOptions()

				r, err := New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(r).ToNot(BeNil())
			})

			It("by default, uses Both mode", func() {
				Expect(opts.Mode).To(Equal(Both))
				Expect(r.Options.Mode).To(Equal(Both))
			})

			It("should error with missing options", func() {
				r, err := New(nil)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("cannot be nil"))
				Expect(r).To(BeNil())
			})

			It("should error with unreachable rabbit server", func() {
				opts := generateOptions()
				opts.URLs = []string{"amqp://bad-url"}

				r, err := New(opts)

				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("unable to dial server"))
				Expect(r).To(BeNil())
			})

			It("happy: it should succeed after having failed on unreachable rabbit server", func() {
				opts := generateOptions()
				opts.URLs = []string{"amqp://bad-url", "amqp://localhost"}

				r, err := New(opts)

				Expect(err).To(BeNil())
				Expect(r).ToNot(BeNil())
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
			errChan = make(chan *ConsumeError, 1)
		)

		When("attempting to consume messages in producer mode", func() {
			It("Consume should not block and immediately return", func() {
				opts.Mode = Producer
				ra, err := New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(ra).ToNot(BeNil())

				var exit bool

				go func() {
					r.Consume(nil, nil, func(m amqp.Delivery) error {
						return nil
					})

					exit = true
				}()

				// Give the goroutine a little to start up
				time.Sleep(50 * time.Millisecond)

				Expect(exit).To(BeTrue())
			})
		})

		When("consuming messages with a context", func() {
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
					Expect(msg.Exchange).To(Equal(opts.Bindings[0].ExchangeName))
					Expect(msg.RoutingKey).To(Equal(opts.Bindings[0].BindingKeys[0]))
					Expect(msg.ConsumerTag).To(Equal(opts.ConsumerTag))

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

				// Publish 10 messages -> cancel -> publish remainder of messages ->
				// verify runfunc was hit only 10 times
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
		When("Mode is Producer", func() {
			It("will return an error", func() {
				opts.Mode = Producer
				ra, err := New(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(ra).ToNot(BeNil())

				err = ra.ConsumeOnce(nil, func(m amqp.Delivery) error { return nil })

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("library is configured in Producer mode"))
			})
		})

		When("passed context is nil", func() {
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
		Context("happy path", func() {
			It("correctly publishes message", func() {
				var receivedMessage *amqp.Delivery

				go func() {
					var err error
					receivedMessage, err = receiveMessage(ch, opts)

					Expect(err).ToNot(HaveOccurred())
				}()

				time.Sleep(25 * time.Millisecond)

				testMessage := []byte(uuid.NewV4().String())
				publishErr := r.Publish(nil, opts.Bindings[0].BindingKeys[0], testMessage)

				Expect(publishErr).ToNot(HaveOccurred())

				// Give our consumer some time to receive the message
				time.Sleep(100 * time.Millisecond)

				Expect(receivedMessage.Body).To(Equal(testMessage))
				Expect(receivedMessage.AppId).To(Equal(opts.AppID))
			})

			When("Mode is Consumer", func() {
				It("should return an error", func() {
					opts.Mode = Consumer
					ra, err := New(opts)

					Expect(err).ToNot(HaveOccurred())
					Expect(ra).ToNot(BeNil())

					err = ra.Publish(nil, "messages", []byte("test"))

					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("library is configured in Consumer mode"))
				})
			})
		})

		When("producer server channel is nil", func() {
			It("will generate a new server channel", func() {
				r.ProducerServerChannel = nil

				var receivedMessage *amqp.Delivery

				go func() {
					var err error
					receivedMessage, err = receiveMessage(ch, opts)

					Expect(err).ToNot(HaveOccurred())
				}()

				time.Sleep(25 * time.Millisecond)

				testMessage := []byte(uuid.NewV4().String())
				publishErr := r.Publish(nil, opts.Bindings[0].BindingKeys[0], testMessage)

				Expect(publishErr).ToNot(HaveOccurred())

				// Give our consumer some time to receive the message
				time.Sleep(100 * time.Millisecond)

				Expect(receivedMessage.Body).To(Equal(testMessage))
			})
		})
	})

	Describe("Stop", func() {
		When("consuming messages via Consume()", func() {
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

	Describe("Close", func() {
		When("called after instantiating new rabbit", func() {
			It("does not error", func() {
				err := r.Close()
				Expect(err).ToNot(HaveOccurred())
			})
		})

		When("called before Consume", func() {
			It("should cause Consume to immediately return", func() {
				err := r.Close()
				Expect(err).ToNot(HaveOccurred())

				// This shouldn't block because internal ctx func should have been called
				r.Consume(nil, nil, func(m amqp.Delivery) error {
					return nil
				})

				Expect(true).To(BeTrue())
			})
		})

		When("called before ConsumeOnce", func() {
			It("ConsumeOnce should timeout", func() {
				err := r.Close()
				Expect(err).ToNot(HaveOccurred())

				// This shouldn't block because internal ctx func should have been called
				err = r.ConsumeOnce(nil, func(m amqp.Delivery) error {
					return nil
				})

				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(ShutdownError))
			})
		})

		When("called before Publish", func() {
			It("Publish should error", func() {
				err := r.Close()
				Expect(err).ToNot(HaveOccurred())

				err = r.Publish(nil, "messages", []byte("testing"))

				Expect(err).To(HaveOccurred())
				Expect(err).To(Equal(ShutdownError))
			})
		})
	})

	Describe("validateOptions", func() {
		Context("validation combinations", func() {
			BeforeEach(func() {
				opts = generateOptions()
			})

			It("errors with nil options", func() {
				err := ValidateOptions(nil)
				Expect(err).To(HaveOccurred())
			})

			It("should error on invalid mode", func() {
				opts.Mode = 15
				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid mode"))
			})

			It("errors when no valid URL is set", func() {
				opts.URLs = []string{"", "", ""}

				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("At least one non-empty URL must be provided"))
			})

			It("errors when no URL is set", func() {
				opts.URLs = []string{}

				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("At least one non-empty URL must be provided"))
			})

			It("errors when no exchange is specified", func() {
				opts.URLs = []string{
					"amqp://whatever",
				}
				// empty/nil bindings
				opts.Bindings = []Binding{}
				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("At least one Exchange must be specified"))
			})

			It("errors when multiple exchanges are specified in Producer/Both mode", func() {
				opts.Bindings = []Binding{
					{
						ExchangeName:    "exchange1",
						ExchangeDeclare: false,
						ExchangeType:    "",
					},
					{
						ExchangeName:    "exchange2",
						ExchangeDeclare: false,
						ExchangeType:    "",
					},
				}
				opts.URLs = []string{
					"amqp://whatever",
				}

				opts.Mode = Producer
				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("Exactly one Exchange must be specified when publishing messages"))

				opts.Mode = Both
				err = ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("Exactly one Exchange must be specified when publishing messages"))
			})

			It("only checks ExchangeType if ExchangeDeclare is true", func() {
				opts.Bindings = []Binding{
					{
						ExchangeName:    "exchange1",
						ExchangeDeclare: false,
						ExchangeType:    "",
						BindingKeys: []string{
							"routingeKey1",
							"routingeKey2",
						},
					},
				}
				opts.URLs = []string{
					"amqp://whatever",
				}
				opts.Mode = Consumer
				err := ValidateOptions(opts)
				Expect(err).ToNot(HaveOccurred())

				opts.Bindings[0].ExchangeDeclare = true
				opts.Bindings[0].ExchangeType = ""

				err = ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ExchangeType cannot be empty"))
			})

			It("errors if ExchangeName is unset", func() {
				opts.Bindings = []Binding{
					{
						ExchangeName: "",
					},
				}
				opts.URLs = []string{
					"amqp://whatever",
				}
				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("ExchangeName cannot be empty"))
			})

			It("errors if BindingKeys is unset", func() {
				opts.Bindings = []Binding{
					{
						ExchangeName: "exchange1",
						BindingKeys:  []string{},
					},
				}
				opts.URLs = []string{
					"amqp://whatever",
				}

				err := ValidateOptions(opts)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("At least one BindingKeys must be specified"))
			})

			It("sets RetryConnect to default if unset", func() {
				opts.RetryReconnectSec = 0

				err := ValidateOptions(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(opts.RetryReconnectSec).To(Equal(DefaultRetryReconnectSec))
			})

			It("sets AppID and ConsumerTag to default if unset", func() {
				opts.AppID = ""
				opts.ConsumerTag = ""

				err := ValidateOptions(opts)

				Expect(err).ToNot(HaveOccurred())
				Expect(opts.ConsumerTag).To(ContainSubstring("c-rabbit-"))
				Expect(opts.AppID).To(ContainSubstring("p-rabbit-"))
			})
		})
	})
})

func generateOptions() *Options {
	exchangeName := "rabbit-" + uuid.NewV4().String()

	return &Options{
		URLs:      []string{"amqp://localhost"},
		QueueName: "rabbit-" + uuid.NewV4().String(),
		Bindings: []Binding{
			{
				ExchangeName:       exchangeName,
				ExchangeType:       "topic",
				ExchangeDeclare:    true,
				ExchangeDurable:    false,
				ExchangeAutoDelete: true,
				BindingKeys:        []string{exchangeName},
			},
		},
		QosPrefetchCount:  0,
		QosPrefetchSize:   0,
		RetryReconnectSec: 10,
		QueueDeclare:      true,
		QueueDurable:      false,
		QueueExclusive:    false,
		QueueAutoDelete:   true,
		AppID:             "rabbit-test-producer",
		ConsumerTag:       "rabbit-test-consumer",
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
	var err error
	var ac *amqp.Connection
	for _, url := range opts.URLs {
		ac, err = amqp.Dial(url)
		if err != nil {
			ac = nil
		} else {
			break
		}
	}
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
		if err := ch.Publish(opts.Bindings[0].ExchangeName, opts.Bindings[0].BindingKeys[0], false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(v),
		}); err != nil {
			return err
		}
	}

	return nil
}

func receiveMessage(ch *amqp.Channel, opts *Options) (*amqp.Delivery, error) {
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

	if err := ch.QueueBind(tmpQueueName, opts.Bindings[0].BindingKeys[0], opts.Bindings[0].ExchangeName, false, nil); err != nil {
		return nil, errors.Wrap(err, "unable to bind queue")
	}

	deliveryChan, err := ch.Consume(tmpQueueName, "", true, false, false, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create delivery channel")
	}

	select {
	case m := <-deliveryChan:
		logrus.Debug("Test: received message in receiveMessage()")
		return &m, nil
	case <-time.After(5 * time.Second):
		logrus.Debug("Test: timed out waiting for message in receiveMessage()")
		return nil, errors.New("timed out")
	}
}
