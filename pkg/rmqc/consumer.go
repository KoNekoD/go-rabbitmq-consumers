package rmqc

import (
	"encoding/json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AbstractConsumer[JobType any] struct {
	/** RabbitMQ */
	dsn string

	/** Exchange */
	exchangeName       string
	exchangeKind       ExchangeKind
	exchangeDurable    bool
	exchangeAutoDelete bool
	exchangeInternal   bool
	exchangeNoWait     bool
	exchangeArgs       amqp.Table

	/** Queue */
	queueName       string
	queueDurable    bool
	queueAutoDelete bool
	queueExclusive  bool
	queueNoWait     bool
	queueArgs       amqp.Table

	/** Queue Bind */
	queueBindKey    string
	queueBindNoWait bool
	queueBindArgs   amqp.Table

	/** QoS */
	prefetchCount int
	prefetchSize  int
	qosGlobal     bool

	/** Consumer */
	consumerTag      string
	consumeAutoAck   bool
	consumeExclusive bool
	consumeNoLocal   bool
	consumeNoWait    bool
	consumeArgs      amqp.Table

	/** Handle Functions */
	handleFunc func(JobType) error
}

type NewOption[JobType any] func(*AbstractConsumer[JobType])

func WithExchangeName[JobType any](exchangeName string) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.exchangeName = exchangeName
	}
}

func WithExchangeKind[JobType any](exchangeKind ExchangeKind) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.exchangeKind = exchangeKind
	}
}

func WithExchangeDurable[JobType any](exchangeDurable bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.exchangeDurable = exchangeDurable
	}
}

func WithExchangeAutoDelete[JobType any](exchangeAutoDelete bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.exchangeAutoDelete = exchangeAutoDelete
	}
}

func WithExchangeInternal[JobType any](exchangeInternal bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.exchangeInternal = exchangeInternal
	}
}

func WithExchangeNoWait[JobType any](exchangeNoWait bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.exchangeNoWait = exchangeNoWait
	}
}

func WithExchangeArgs[JobType any](exchangeArgs amqp.Table) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.exchangeArgs = exchangeArgs
	}
}

func WithQueueDurable[JobType any](queueDurable bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.queueDurable = queueDurable
	}
}

func WithQueueAutoDelete[JobType any](queueAutoDelete bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.queueAutoDelete = queueAutoDelete
	}
}

func WithQueueExclusive[JobType any](queueExclusive bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.queueExclusive = queueExclusive
	}
}

func WithQueueNoWait[JobType any](queueNoWait bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.queueNoWait = queueNoWait
	}
}

func WithQueueArgs[JobType any](queueArgs amqp.Table) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.queueArgs = queueArgs
	}
}

func WithQueueBindKey[JobType any](queueBindKey string) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.queueBindKey = queueBindKey
	}
}

func WithQueueBindNoWait[JobType any](queueBindNoWait bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.queueBindNoWait = queueBindNoWait
	}
}

func WithQueueBindArgs[JobType any](queueBindArgs amqp.Table) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.queueBindArgs = queueBindArgs
	}
}

func WithConsumerTag[JobType any](consumerTag string) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.consumerTag = consumerTag
	}
}

func WithConsumeAutoAck[JobType any](consumeAutoAck bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.consumeAutoAck = consumeAutoAck
	}
}

func WithConsumeExclusive[JobType any](consumeExclusive bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.consumeExclusive = consumeExclusive
	}
}

func WithConsumeNoLocal[JobType any](consumeNoLocal bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.consumeNoLocal = consumeNoLocal
	}
}

func WithConsumeNoWait[JobType any](consumeNoWait bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.consumeNoWait = consumeNoWait
	}
}

func WithConsumeArgs[JobType any](consumeArgs amqp.Table) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.consumeArgs = consumeArgs
	}
}

func WithPrefetchCount[JobType any](prefetchCount int) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.prefetchCount = prefetchCount
	}
}

func WithPrefetchSize[JobType any](prefetchSize int) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.prefetchSize = prefetchSize
	}
}

func WithQosGlobal[JobType any](qosGlobal bool) NewOption[JobType] {
	return func(c *AbstractConsumer[JobType]) {
		c.qosGlobal = qosGlobal
	}
}

func NewAbstractConsumer[JobType any](
	dsn string,
	options ...NewOption[JobType],
) AbstractConsumer[JobType] {
	cleanDsn, queueName := unwrapQueueFromDSN(dsn)

	c := AbstractConsumer[JobType]{
		dsn:           cleanDsn,
		exchangeName:  queueName,
		exchangeKind:  ExchangeFanout,
		queueName:     queueName,
		consumerTag:   queueName,
		prefetchCount: 1,
	}

	for _, option := range options {
		option(&c)
	}

	return c
}

func SetupConsumerChild[JobType any, ChildType Consumer[JobType]](
	parent *AbstractConsumer[JobType],
	child ChildType,
) ChildType {
	parent.SetHandleFunc(child.Handle)

	return child
}

func (c *AbstractConsumer[JobType]) SetHandleFunc(handleFunc func(JobType) error) {
	c.handleFunc = handleFunc
}

type Consumer[JobType any] interface {
	Handle(JobType) error
}

func (c *AbstractConsumer[JobType]) Init() error {
	connection, err := amqp.Dial(c.dsn)
	if err != nil {
		return errors.WithStack(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		return errors.WithStack(err)
	}

	err = channel.ExchangeDeclare(
		c.exchangeName,
		string(c.exchangeKind),
		c.exchangeDurable,
		c.exchangeAutoDelete,
		c.exchangeInternal,
		c.exchangeNoWait,
		c.exchangeArgs,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	_, err = channel.QueueDeclare(
		c.queueName,
		c.queueDurable,
		c.queueAutoDelete,
		c.queueExclusive,
		c.queueNoWait,
		c.queueArgs,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	err = channel.QueueBind(
		c.queueName,
		c.queueBindKey,
		c.exchangeName,
		c.queueBindNoWait,
		c.queueBindArgs,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	err = channel.Qos(
		c.prefetchCount,
		c.prefetchSize,
		c.qosGlobal,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	delivery, err := channel.Consume(
		c.queueName,
		c.consumerTag,
		c.consumeAutoAck,
		c.consumeExclusive,
		c.consumeNoLocal,
		c.consumeNoWait,
		c.consumeArgs,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	var multiErr *multierror.Error

	for msg := range delivery {
		var job JobType

		err = json.Unmarshal(msg.Body, &job)
		if err != nil {
			multiErr = multierror.Append(multiErr, errors.WithStack(err))
			break
		}

		err = c.handleFunc(job)
		if err != nil {
			multiErr = multierror.Append(multiErr, errors.WithStack(err))
			break
		}

		err = msg.Ack(false)
		if err != nil {
			multiErr = multierror.Append(multiErr, errors.WithStack(err))
			break
		}
	}

	err = channel.Cancel(c.consumerTag, false)
	if err != nil {
		multiErr = multierror.Append(multiErr, errors.WithStack(err))
	}

	err = channel.Close()
	if err != nil {
		multiErr = multierror.Append(multiErr, errors.WithStack(err))
	}

	err = connection.Close()
	if err != nil {
		multiErr = multierror.Append(multiErr, errors.WithStack(err))
	}

	if multiErr != nil {
		return errors.WithStack(multiErr)
	}

	return nil
}
