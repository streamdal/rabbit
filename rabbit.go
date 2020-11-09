// Package rabbit is a simple streadway/amqp wrapper library that comes with:
//
// * Auto-reconnect support
//
// * Multi-connector mode
//
// * Context support
//
// * Helpers for consuming once or forever and publishing
//
// The library is used internally at https://batch.sh where it powers most of
// the platform's backend services.
//
// For an example, refer to the README.md.
package rabbit

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/relistan/go-director"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

const (
	// How long to wait before attempting to reconnect to a rabbit server
	DefaultRetryReconnectSec = 60

	Both     Mode = 0
	Consumer Mode = 1
	Producer Mode = 2
)

var (
	// Used for identifying named consumer
	DefaultNamePrefix = "n-rabbit"

	// Used for identifying consumer
	DefaultConsumerTagPrefix = "c-rabbit"

	// Used for identifying producer
	DefaultAppIDPrefix = "p-rabbit"
)

// IRabbit is the interface that the `rabbit` library implements. It's here as
// convenience.
type IRabbit interface {
	Consume(ctx context.Context, errChan chan *ConsumeError, f func(name string, msg amqp.Delivery) error)
	ConsumeOnce(ctx context.Context, runFunc func(name string, msg amqp.Delivery) error) error
	Publish(ctx context.Context, routingKey string, payload []byte) error
	Stop() error
	Close() error
}

// Rabbit struct that is instantiated via `New()`. You should not instantiate
// this struct by hand (unless you have a really good reason to do so).
type Rabbit struct {
	Conns                    map[string]*amqp.Connection
	ConsumerDeliveryChannels map[string]<-chan amqp.Delivery
	ConsumerRWMutexMap       map[string]*sync.RWMutex
	NotifyCloseChans         map[string]chan *amqp.Error
	ProducerServerChannels   map[string]*amqp.Channel
	ProducerRWMutexMap       map[string]*sync.RWMutex
	ConsumeLooper            director.Looper
	Options                  map[string]*Options

	ctx    context.Context
	cancel func()
	log    *logrus.Entry
}

type Mode int

// Options determines how the `rabbit` library will behave and should be passed
// in to rabbit via `New()`. Many of the options are optional (and will fall
// back to sane defaults).
type Options struct {
	// Used for identifying different specific consumers/producers
	// (only relevant if using rabbit in "multi-connector" mode)
	Name string

	// Required; format "amqp://user:pass@host:port"
	URL string

	// In what mode does the library operate (Both, Consumer, Producer)
	Mode Mode

	// If left empty, server will auto generate queue name
	QueueName string

	// Required
	ExchangeName string

	// Used as either routing (publish) or binding key (consume)
	RoutingKey string

	// Whether to declare/create exchange on connect
	ExchangeDeclare bool

	// Required if declaring queue (valid: direct, fanout, topic, headers)
	ExchangeType string

	// Whether exchange should survive/persist server restarts
	ExchangeDurable bool

	// Whether to delete exchange when its no longer used; used only if ExchangeDeclare set to true
	ExchangeAutoDelete bool

	// https://godoc.org/github.com/streadway/amqp#Channel.Qos
	// Leave unset if no QoS preferences
	QosPrefetchCount int
	QosPrefetchSize  int

	// How long to wait before we retry connecting to a server (after disconnect)
	RetryReconnectSec int

	// Whether queue should survive/persist server restarts (and there are no remaining bindings)
	QueueDurable bool

	// Whether consumer should be the sole consumer of the queue; used only if
	// QueueDeclare set to true
	QueueExclusive bool

	// Whether to delete queue on consumer disconnect; used only if QueueDeclare set to true
	QueueAutoDelete bool

	// Whether to declare/create queue on connect; used only if QueueDeclare set to true
	QueueDeclare bool

	// Whether to automatically acknowledge consumed message(s)
	AutoAck bool

	// Used for identifying consumer
	ConsumerTag string

	// Used as a property to identify producer
	AppID string
}

// ConsumeError will be passed down the error channel if/when `f()` func runs
// into an error during `Consume()`.
type ConsumeError struct {
	// Name of the consumer that received the error
	Name    string
	Message *amqp.Delivery
	Error   error
}

// New is used for instantiating the library.
func New(opts ...*Options) (*Rabbit, error) {
	if err := ValidateOptions(opts); err != nil {
		return nil, errors.Wrap(err, "invalid options")
	}

	// Make options a bit easier to search
	optsMap := make(map[string]*Options, 0)

	for _, o := range opts {
		optsMap[o.Name] = o
	}

	// Connect to all rabbit instances, create notification channels
	acs := make(map[string]*amqp.Connection, 0)
	notificationChannels := make(map[string]chan *amqp.Error, 0)

	for _, opt := range opts {
		ac, err := amqp.Dial(opt.URL)
		if err != nil {
			return nil, errors.Wrap(err, "unable to dial server(s)")
		}

		acs[opt.Name] = ac

		// Create close notification channel
		notificationChannels[opt.Name] = make(chan *amqp.Error)
	}

	// Used for stopping consume via Stop()
	ctx, cancel := context.WithCancel(context.Background())

	r := &Rabbit{
		Conns:                    acs,
		ConsumerDeliveryChannels: make(map[string]<-chan amqp.Delivery, 0),
		ProducerServerChannels:   make(map[string]*amqp.Channel, 0),
		ConsumerRWMutexMap:       make(map[string]*sync.RWMutex),
		ProducerRWMutexMap:       make(map[string]*sync.RWMutex),
		NotifyCloseChans:         notificationChannels,
		ConsumeLooper:            director.NewFreeLooper(director.FOREVER, make(chan error, 1)),
		Options:                  optsMap,

		ctx:    ctx,
		cancel: cancel,
		log:    logrus.WithField("pkg", "rabbit"),
	}

	// Create mutexes
	for _, o := range opts {
		r.ConsumerRWMutexMap[o.Name] = &sync.RWMutex{}
		r.ProducerRWMutexMap[o.Name] = &sync.RWMutex{}
	}

	// Only create consumer channels if we are in "Consumer" or "Both" mode
	if err := r.createConsumerChannels(); err != nil {
		return nil, errors.Wrap(err, "unable to create initial consumer channel(s)")
	}

	// Configure all connections to notify us about any happenings
	for name, conn := range acs {
		conn.NotifyClose(r.NotifyCloseChans[name])
	}

	// Launch connection watchers
	for name, ch := range r.NotifyCloseChans {
		go r.watchNotifyClose(name, ch)
	}

	return r, nil
}

// ValidateOptions validates various combinations of options.
func ValidateOptions(opts []*Options) error {
	if len(opts) < 1 {
		return errors.New("at least one set of options must be provided")
	}

	names := make(map[string]bool, 0)

	for i, o := range opts {
		if o == nil {
			return errors.New("Options cannot be nil")
		}

		if o.Name == "" {
			o.Name = DefaultNamePrefix + "-" + uuid.NewV4().String()[0:8]
		} else {
			if _, ok := names[o.Name]; ok {
				return fmt.Errorf("name '%s' in option config must be unique", o.Name)
			}

			names[o.Name] = true
		}

		if o.URL == "" {
			return errors.New("URL cannot be empty")
		}

		if o.ExchangeDeclare {
			if o.ExchangeType == "" {
				return errors.New("ExchangeType cannot be empty if ExchangeDeclare set to true")
			}
		}

		if o.ExchangeName == "" {
			return errors.New("ExchangeName cannot be empty")
		}

		if o.RoutingKey == "" {
			return errors.New("RoutingKey cannot be empty")
		}

		if o.RetryReconnectSec == 0 {
			o.RetryReconnectSec = DefaultRetryReconnectSec
		}

		if o.AppID == "" {
			o.AppID = DefaultAppIDPrefix + "-" + uuid.NewV4().String()[0:8]
		}

		if o.ConsumerTag == "" {
			o.ConsumerTag = DefaultConsumerTagPrefix + "-" + uuid.NewV4().String()[0:8]
		}

		validModes := []Mode{Both, Producer, Consumer}

		var found bool

		for _, validMode := range validModes {
			if validMode == o.Mode {
				found = true
			}
		}

		if !found {
			return fmt.Errorf("invalid mode '%d' for option cfg #%d", o.Mode, i)
		}
	}

	return nil
}

// Consume consumes messages from all configured queues and executes `f` for
// every received message.
//
// `Consume()` will block until it is stopped either via the passed in `ctx` OR
// by calling `Stop()`
//
// It is also possible to see the errors that `f()` runs into by passing in an
// error channel (`chan *ConsumeError`).
//
// Both `ctx` and `errChan` can be `nil`.
//
// If the server goes away, `Consume` will automatically attempt to reconnect.
// Subsequent reconnect attempts will sleep/wait for `DefaultRetryReconnectSec`
// between attempts.
func (r *Rabbit) Consume(ctx context.Context, errChan chan *ConsumeError, f func(name string, msg amqp.Delivery) error) {
	if len(r.ConsumerDeliveryChannels) < 1 {
		r.log.Error("unable to Consume() - library has no configured ConsumerDeliveryChannels")
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	selectChannels := make([]reflect.SelectCase, 0)
	nameIndex := make([]string, 0)

	// To listen to N number of channels, we must use reflect.Select.
	// In addition to returning a value from one of the channels, reflect.Select
	// also returns the _index_ of the channel we received a message on. We use
	// this index to lookup the name of the consumer which we then pass to the
	// ConsumeFunc.
	for name, ch := range r.ConsumerDeliveryChannels {
		selectChannel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}

		selectChannels = append(selectChannels, selectChannel)
		nameIndex = append(nameIndex, name)
	}

	// We also need to append the input ctx channel and our internal Stop() chan
	// as we will no longer using select {} and instead use reflect.Select.
	selectChannels = append(selectChannels, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	inputContextIndex := len(selectChannels) + 1

	selectChannels = append(selectChannels, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.ctx.Done())})
	internalContextIndex := len(selectChannels) + 1

	r.log.Debug("waiting for messages from rabbit ...")

	var quit bool

	remaining := len(selectChannels)

	r.ConsumeLooper.Loop(func() error {
		// This is needed to prevent context flood in case .Quit() wasn't picked
		// up quickly enough by director
		if quit {
			time.Sleep(25 * time.Millisecond)
			return nil
		}

		if remaining == 0 {
			r.log.Warning("no more remaining select channels - time to quit")
			r.ConsumeLooper.Quit()
			quit = true
		}

		index, value, ok := reflect.Select(selectChannels)

		if !ok {
			r.log.Warning("delivery channel for has been closed")

			selectChannels[index].Chan = reflect.ValueOf(nil)
			remaining -= 1
			return nil
		}

		switch index {
		case inputContextIndex:
			r.log.Warning("stopped via input context")
			r.ConsumeLooper.Quit()
			quit = true
		case internalContextIndex:
			r.log.Warning("stopped via Stop()")
			r.ConsumeLooper.Quit()
			quit = true
		default:
			name := nameIndex[index]

			msg, ok := value.Interface().(amqp.Delivery)
			//msg, ok := reflect.ValueOf(value).Interface().(amqp.Delivery)
			if !ok {
				r.log.Errorf("unable to type assert delivery msg")

				if errChan != nil {
					// Write in a goroutine in case error channel is not consumed fast enough
					go func() {
						errChan <- &ConsumeError{
							Message: nil,
							Error:   fmt.Errorf("unable to type assert delivery msg for '%s'", name),
						}
					}()
				}
			}

			if err := f(name, msg); err != nil {
				r.log.Debugf("error during func exec for '%s': %s", name, err)

				if errChan != nil {
					// Write in a goroutine in case error channel is not consumed fast enough
					go func() {
						errChan <- &ConsumeError{
							Message: &msg,
							Error:   err,
						}
					}()
				}
			}
		}

		return nil
	})

	r.log.Debug("Consume finished - exiting")
}

// ConsumeOnce will consume exactly one message from the configured queue,
// execute `runFunc()` on the message and return.
//
// Same as with `Consume()`, you can pass in a context to cancel `ConsumeOnce()`
// or run `Stop()`.
func (r *Rabbit) ConsumeOnce(ctx context.Context, runFunc func(name string, msg amqp.Delivery) error) error {
	if len(r.ConsumerDeliveryChannels) < 1 {
		panic("unable to Consume() - library has no configured ConsumerDeliveryChannels")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	selectChannels := make([]reflect.SelectCase, 0)
	nameIndex := make([]string, 0)

	// To listen to N number of channels, we must use reflect.Select.
	// In addition to returning a value from one of the channels, reflect.Select
	// also returns the _index_ of the channel we received a message on. We use
	// this index to lookup the name of the consumer which we then pass to the
	// ConsumeFunc.
	for name, ch := range r.ConsumerDeliveryChannels {
		selectChannel := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}

		selectChannels = append(selectChannels, selectChannel)
		nameIndex = append(nameIndex, name)
	}

	// We also need to append the input ctx channel and our internal Stop() chan
	// as we will no longer using select {} and instead use reflect.Select.
	selectChannels = append(selectChannels, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	inputContextIndex := len(selectChannels) + 1

	selectChannels = append(selectChannels, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.ctx.Done())})
	internalContextIndex := len(selectChannels) + 1

	remaining := len(selectChannels)

	r.log.Debug("waiting for a single message from rabbit ...")

	index, value, ok := reflect.Select(selectChannels)

	if !ok {
		r.log.Warningf("delivery channel for '%s' has been closed", nameIndex[index])

		selectChannels[index].Chan = reflect.ValueOf(nil)
		remaining -= 1
		return nil
	}

	switch index {
	case inputContextIndex:
		r.log.Warning("stopped via input context")
		return nil
	case internalContextIndex:
		r.log.Warning("stopped via Stop()")
		return nil
	default:
		name := nameIndex[index]

		msg, ok := reflect.ValueOf(value).Interface().(amqp.Delivery)
		if !ok {
			r.log.Errorf("unable to type assert delivery msg for '%s'", name)

			return errors.New("unable to type assert deliver msg")
		}

		if err := runFunc(name, msg); err != nil {
			r.log.Debugf("error during func exec for '%s': %s", name, err)
			return fmt.Errorf("error during func exec for '%s': %s", name, err)
		}
	}

	r.log.Debug("ConsumeOnce finished - exiting")

	return nil
}

// Publish publishes one message to ALL producer server channels.
//
// NOTE: Context semantics are not implemented.
func (r *Rabbit) Publish(ctx context.Context, routingKey string, body []byte) error {
	if len(r.ProducerServerChannels) < 1 {
		return errors.New("unable to Publish - library has no producer server channels")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	//// Is this the first time we're publishing?
	//if r.ProducerServerChannel == nil {
	//	ch, err := r.newServerChannel()
	//	if err != nil {
	//		return errors.Wrap(err, "unable to create server channel")
	//	}
	//
	//	r.ProducerRWMutex.Lock()
	//	r.ProducerServerChannel = ch
	//	r.ProducerRWMutex.Unlock()
	//}

	errs := make([]string, 0)

	for name, channel := range r.ProducerServerChannels {
		r.ProducerRWMutexMap[name].RLock()

		if err := channel.Publish(r.Options[name].ExchangeName, routingKey, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         body,
			AppId:        r.Options[name].AppID,
		}); err != nil {
			r.log.Errorf("unable to publish to '%s': %s", name, err)
			errs = append(errs, fmt.Sprintf("unable to publish to '%s': %s", name, err))
		}

		r.ProducerRWMutexMap[name].RUnlock()
	}

	if len(errs) != 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

// Stop stops an in-progress `Consume()` or `ConsumeOnce()`.
func (r *Rabbit) Stop() error {
	r.cancel()
	return nil
}

// Close stops any active Consume and closes the amqp connection (and channels using the conn)
//
// You should re-instantiate the rabbit lib once this is called.
func (r *Rabbit) Close() error {
	r.cancel()

	errs := make([]string, 0)

	for name, conn := range r.Conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("unable to close '%s' amqp connection: %s", name, err))
		}
	}

	if len(errs) != 0 {
		return errors.New(strings.Join(errs, "; "))
	}

	return nil
}

func (r *Rabbit) watchNotifyClose(name string, notifyCloseChan chan *amqp.Error) {
	// TODO: Use a looper here
	for {
		closeErr := <-notifyCloseChan

		r.log.Debugf("received message on notify close channel for '%s': '%+v' (reconnecting)",
			name, closeErr)

		// Acquire mutex to pause all consumers/producers while we reconnect AND
		// prevent access to the channel map
		r.ConsumerRWMutexMap[name].Lock()
		r.ProducerRWMutexMap[name].Lock()

		var attempts int

		for {
			attempts++

			if err := r.reconnect(name); err != nil {
				r.log.Warningf("unable to complete reconnect for '%s': %s; retrying in %d",
					name, err, r.Options[name].RetryReconnectSec)

				time.Sleep(time.Duration(r.Options[name].RetryReconnectSec) * time.Second)
				continue
			}

			r.log.Debugf("successfully reconnected '%s' after %d attempts", name, attempts)
			break
		}

		// Create and set a new notify close channel (since old one gets closed)
		r.NotifyCloseChans[name] = make(chan *amqp.Error, 0)
		r.Conns[name].NotifyClose(r.NotifyCloseChans[name])

		// Update channel
		if r.Options[name].Mode == Producer {
			serverChannel, err := r.newServerChannel(name)
			if err != nil {
				logrus.Errorf("unable to set new channel: %s", err)
				panic(fmt.Sprintf("unable to set new channel: %s", err))
			}

			r.ProducerServerChannels[name] = serverChannel
		} else {
			deliveryChan, serverChan, err := r.newConsumerChannel(name)
			if err != nil {
				logrus.Errorf("unable to set new channel for '%s': %s", name, err)

				// TODO: This is super shitty. Should address this.
				panic(fmt.Sprintf("unable to set new channel for '%s': %s", name, err))
			}

			r.ConsumerDeliveryChannels[name] = deliveryChan
			r.ProducerServerChannels[name] = serverChan
		}

		// Unlock so that consumers/producers can begin reading messages from a new channel
		r.ConsumerRWMutexMap[name].Unlock()
		r.ProducerRWMutexMap[name].Unlock()

		r.log.Debug("watchNotifyClose has completed successfully")
	}
}

func (r *Rabbit) newServerChannel(name string) (*amqp.Channel, error) {
	if r.Conns[name] == nil {
		return nil, errors.New("Conn is nil - did this get instantiated correctly? bug?")
	}

	ch, err := r.Conns[name].Channel()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate channel")
	}

	if err := ch.Qos(r.Options[name].QosPrefetchCount, r.Options[name].QosPrefetchSize, false); err != nil {
		return nil, errors.Wrap(err, "unable to set qos policy")
	}

	if r.Options[name].ExchangeDeclare {
		if err := ch.ExchangeDeclare(
			r.Options[name].ExchangeName,
			r.Options[name].ExchangeType,
			r.Options[name].ExchangeDurable,
			r.Options[name].ExchangeAutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return nil, errors.Wrap(err, "unable to declare exchange")
		}
	}

	if r.Options[name].Mode == Both || r.Options[name].Mode == Consumer {
		if r.Options[name].QueueDeclare {
			if _, err := ch.QueueDeclare(
				r.Options[name].QueueName,
				r.Options[name].QueueDurable,
				r.Options[name].QueueAutoDelete,
				r.Options[name].QueueExclusive,
				false,
				nil,
			); err != nil {
				return nil, err
			}
		}

		if err := ch.QueueBind(
			r.Options[name].QueueName,
			r.Options[name].RoutingKey,
			r.Options[name].ExchangeName,
			false,
			nil,
		); err != nil {
			return nil, errors.Wrap(err, "unable to bind queue")
		}
	}

	return ch, nil
}

func (r *Rabbit) createConsumerChannels() error {
	for _, o := range r.Options {
		if o.Mode == Producer {
			continue
		}

		// We are either Consumer or Both
		deliveryChan, serverChan, err := r.newConsumerChannel(o.Name)
		if err != nil {
			return fmt.Errorf("unable to create consumer channel for '%s': %s", o.Name, err)
		}

		r.ConsumerDeliveryChannels[o.Name] = deliveryChan
		r.ProducerServerChannels[o.Name] = serverChan
	}

	return nil
}

func (r *Rabbit) newConsumerChannel(name string) (<-chan amqp.Delivery, *amqp.Channel, error) {
	serverChannel, err := r.newServerChannel(name)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create new server channel")
	}

	deliveryChannel, err := serverChannel.Consume(
		r.Options[name].QueueName,
		r.Options[name].ConsumerTag,
		r.Options[name].AutoAck,
		r.Options[name].QueueExclusive,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to create delivery channel")
	}

	return deliveryChannel, serverChannel, nil
}

func (r *Rabbit) reconnect(name string) error {
	ac, err := amqp.Dial(r.Options[name].URL)
	if err != nil {
		return err
	}

	r.Conns[name] = ac

	return nil
}
