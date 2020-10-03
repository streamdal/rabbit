package rabbit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	DefaultRetryReconnectSec = 60
)

type IRabbit interface {
	Consume(ctx context.Context, errChan chan *ConsumeError, f func(msg amqp.Delivery) error)
	ConsumeOnce(ctx context.Context, runFunc func(msg amqp.Delivery) error) error
	Publish(ctx context.Context, routingKey string, payload []byte) error
	Stop() error
}

type Rabbit struct {
	Conn                    *amqp.Connection
	ConsumerServerChannel   *amqp.Channel
	ConsumerDeliveryChannel <-chan amqp.Delivery
	ConsumerRWMutex         *sync.RWMutex
	NotifyCloseChan         chan *amqp.Error
	ProducerServerChannel   *amqp.Channel
	ProducerRWMutex         *sync.RWMutex
	Looper                  director.Looper
	Options                 *Options

	ctx    context.Context
	cancel func()
	log    *logrus.Entry
}

type Options struct {
	URL               string
	QueueName         string
	ExchangeName      string
	ExchangeType      string
	RoutingKey        string
	QosPrefetchCount  int
	QosPrefetchSize   int
	RetryReconnectSec int
	QueueDurable      bool
	QueueExclusive    bool
	QueueAutoDelete   bool
}

type ConsumeError struct {
	Message *amqp.Delivery
	Error   error
}

func New(opts *Options) (*Rabbit, error) {
	if err := validateOptions(opts); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}

	ac, err := amqp.Dial(opts.URL)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &Rabbit{
		Conn:            ac,
		ConsumerRWMutex: &sync.RWMutex{},
		NotifyCloseChan: make(chan *amqp.Error),
		ProducerRWMutex: &sync.RWMutex{},
		Looper:          director.NewFreeLooper(director.FOREVER, make(chan error)),
		Options:         opts,

		ctx:    ctx,
		cancel: cancel,
		log:    logrus.WithField("pkg", "rabbit"),
	}

	if err := r.newConsumerChannels(); err != nil {
		return nil, errors.Wrap(err, "unable to get initial delivery channel")
	}

	ac.NotifyClose(r.NotifyCloseChan)

	// Launch connection watcher/reconnect
	go r.watchNotifyClose()

	return r, nil
}

func validateOptions(opts *Options) error {
	if opts.URL == "" {
		return errors.New("URL cannot be empty")
	}

	if opts.ExchangeType == "" {
		return errors.New("ExchangeType cannot be empty")
	}

	if opts.ExchangeName == "" {
		return errors.New("ExchangeName cannot be empty")
	}

	if opts.RoutingKey == "" {
		return errors.New("RoutingKey cannot be empty")
	}

	if opts.RetryReconnectSec == 0 {
		opts.RetryReconnectSec = DefaultRetryReconnectSec
	}

	return nil
}

// Consume message from queue and run given func forever (until you call Stop())
func (r *Rabbit) Consume(ctx context.Context, errChan chan *ConsumeError, f func(msg amqp.Delivery) error) {
	r.log.Debug("waiting for messages from rabbit ...")

	var quit bool

	r.Looper.Loop(func() error {
		// This is needed in case .Quit() wasn't picked up quickly enough
		if quit {
			time.Sleep(100 * time.Millisecond)
			return nil
		}

		select {
		case msg := <-r.delivery():
			if err := f(msg); err != nil {
				r.log.Debugf("error during consume: %s", err)

				if errChan != nil {
					go func() {
						errChan <- &ConsumeError{
							Message: &msg,
							Error:   err,
						}
					}()
				}
			}
		case <-ctx.Done():
			r.log.Warning("stopped via context")
			r.Looper.Quit()
		case <-r.ctx.Done():
			r.log.Warning("stopped via Stop()")
			r.Looper.Quit()
			quit = true
		}
		return nil
	})

	r.log.Debug("Consume exiting")
}

func (r *Rabbit) ConsumeOnce(ctx context.Context, runFunc func(msg amqp.Delivery) error) error {
	r.log.Debug("waiting for a single message from rabbit ...")

	select {
	case msg := <-r.delivery():
		if err := runFunc(msg); err != nil {
			return err
		}
	case <-ctx.Done():
		r.log.Warning("received notice to quit")
		return nil
	case <-r.ctx.Done():
		r.log.Warning("stopped via Stop()")
		return nil
	}

	r.log.Debug("ConsumeOnce finished - exiting")

	return nil
}

func (r *Rabbit) Publish(ctx context.Context, routingKey string, body []byte) error {
	// Is this the first time we're publishing?
	if r.ProducerServerChannel == nil {
		ch, err := r.newServerChannel()
		if err != nil {
			return errors.Wrap(err, "unable to create server channel")
		}

		r.ProducerRWMutex.Lock()
		r.ProducerServerChannel = ch
		r.ProducerRWMutex.Unlock()
	}

	r.ProducerRWMutex.RLock()
	defer r.ProducerRWMutex.RUnlock()

	if err := r.ProducerServerChannel.Publish(r.Options.ExchangeName, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		return err
	}

	return nil
}

func (r *Rabbit) Stop() error {
	r.cancel()

	return nil
}

func (r *Rabbit) watchNotifyClose() {
	for {
		closeErr := <-r.NotifyCloseChan

		r.log.Debugf("received message on notify close channel: '%+v' (reconnecting)", closeErr)

		// Acquire mutex to pause all consumers while we reconnect AND prevent
		// access to the channel map
		r.ConsumerRWMutex.Lock()

		var attempts int

		for {
			attempts++

			if err := r.reconnect(); err != nil {
				r.log.Warningf("unable to complete reconnect: %s; retrying in %d", err, r.Options.RetryReconnectSec)
				time.Sleep(time.Duration(r.Options.RetryReconnectSec) * time.Second)
				continue
			}

			r.log.Debugf("successfully reconnected after %d attempts", attempts)
			break
		}

		// Create and set a new notify close channel (since old one gets closed)
		r.NotifyCloseChan = make(chan *amqp.Error, 0)
		r.Conn.NotifyClose(r.NotifyCloseChan)

		// Update channel
		if err := r.newConsumerChannels(); err != nil {
			logrus.Errorf("unable to set new channel: %s", err)

			// TODO: This is super shitty. Should address this.
			panic(fmt.Sprintf("unable to set new channel: %s", err))
		}

		// Unlock so that consumers can begin reading messages from a new channel
		r.ConsumerRWMutex.Unlock()

		r.log.Debug("watchNotifyClose has completed successfully")
	}
}

func (r *Rabbit) newServerChannel() (*amqp.Channel, error) {
	if r.Conn == nil {
		return nil, errors.New("r.Conn is nil - did this get instantiated correctly? bug?")
	}

	ch, err := r.Conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

	if err := ch.Qos(r.Options.QosPrefetchCount, r.Options.QosPrefetchSize, false); err != nil {
		return nil, errors.Wrap(err, "unable to set qos policy")
	}

	if err := ch.ExchangeDeclare(
		r.Options.ExchangeName,
		r.Options.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, errors.Wrap(err, "unable to declare exchange")
	}

	if _, err := ch.QueueDeclare(
		r.Options.QueueName,
		r.Options.QueueDurable,
		r.Options.QueueAutoDelete,
		r.Options.QueueExclusive,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	if err := ch.QueueBind(
		r.Options.QueueName,
		r.Options.RoutingKey,
		r.Options.ExchangeName,
		false,
		nil,
	); err != nil {
		return nil, errors.Wrap(err, "unable to bind queue")
	}

	return ch, nil
}

func (r *Rabbit) newConsumerChannels() error {
	serverChannel, err := r.newServerChannel()
	if err != nil {
		return errors.Wrap(err, "unable to create new server channel")
	}

	deliveryChannel, err := serverChannel.Consume(r.Options.QueueName,
		"",
		false,
		r.Options.QueueExclusive,
		false,
		false,
		nil,
	)

	if err != nil {
		return errors.Wrap(err, "unable to create delivery channel")
	}

	r.ConsumerServerChannel = serverChannel
	r.ConsumerDeliveryChannel = deliveryChannel

	return nil
}

func (r *Rabbit) reconnect() error {
	ac, err := amqp.Dial(r.Options.URL)
	if err != nil {
		return err
	}

	r.Conn = ac

	return nil
}

func (r *Rabbit) delivery() <-chan amqp.Delivery {
	// Acquire lock (in case we are reconnecting and channels are being swapped)
	r.ConsumerRWMutex.RLock()
	defer r.ConsumerRWMutex.RUnlock()

	return r.ConsumerDeliveryChannel
}
