package main

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"

	"github.com/streamdal/rabbit"
)

func main() {
	llog := logrus.New()
	llog.SetLevel(logrus.DebugLevel)

	// Create rabbit instance
	r, err := setup(llog)
	if err != nil {
		llog.Fatalf("Unable to setup rabbit: %s", err)
	}

	errChan := make(chan *rabbit.ConsumeError, 1)

	llog.Debug("Starting error listener...")

	// Launch an error listener
	go func() {
		for {
			select {
			case err := <-errChan:
				llog.Debugf("Received rabbit error: %v", err)
			}
		}
	}()

	llog.Debug("Running consumer...")

	// Run a consumer
	r.Consume(context.Background(), errChan, func(d amqp.Delivery) error {
		llog.Debugf("[Received message]\nHeaders: %v\nBody: %s\n", d.Headers, d.Body)

		// Acknowledge the message
		if err := d.Ack(false); err != nil {
			llog.Errorf("Error acknowledging message: %s", err)
		}

		return nil
	})
}

func setup(logger *logrus.Logger) (*rabbit.Rabbit, error) {
	return rabbit.New(&rabbit.Options{
		URLs:      []string{"amqp://guest:guest@localhost:5672/"},
		Mode:      rabbit.Both,
		QueueName: "test-queue",
		Bindings: []rabbit.Binding{
			{
				ExchangeName:       "test-exchange",
				BindingKeys:        []string{"test-key"},
				ExchangeDeclare:    true,
				ExchangeType:       "topic",
				ExchangeDurable:    true,
				ExchangeAutoDelete: true,
			},
		},
		RetryReconnectSec: 1,
		QueueDurable:      true,
		QueueExclusive:    false,
		QueueAutoDelete:   true,
		QueueDeclare:      true,
		AutoAck:           false,
		ConsumerTag:       "rabbit-example",
		Log:               logger,
	})
}
