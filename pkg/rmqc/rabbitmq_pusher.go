package rmqc

import (
	"encoding/json"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Pusher struct {
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
}

type PusherOption func(*Pusher)

func WithPusherExchangeName(exchangeName string) PusherOption {
	return func(c *Pusher) {
		c.exchangeName = exchangeName
	}
}

func WithPusherExchangeKind(exchangeKind ExchangeKind) PusherOption {
	return func(c *Pusher) {
		c.exchangeKind = exchangeKind
	}
}

func WithPusherExchangeDurable(exchangeDurable bool) PusherOption {
	return func(c *Pusher) {
		c.exchangeDurable = exchangeDurable
	}
}

func WithPusherExchangeAutoDelete(exchangeAutoDelete bool) PusherOption {
	return func(c *Pusher) {
		c.exchangeAutoDelete = exchangeAutoDelete
	}
}

func WithPusherExchangeInternal(exchangeInternal bool) PusherOption {
	return func(c *Pusher) {
		c.exchangeInternal = exchangeInternal
	}
}

func WithPusherExchangeNoWait(exchangeNoWait bool) PusherOption {
	return func(c *Pusher) {
		c.exchangeNoWait = exchangeNoWait
	}
}

func WithPusherExchangeArgs(exchangeArgs amqp.Table) PusherOption {
	return func(c *Pusher) {
		c.exchangeArgs = exchangeArgs
	}
}

func WithPusherQueueDurable(queueDurable bool) PusherOption {
	return func(c *Pusher) {
		c.queueDurable = queueDurable
	}
}

func WithPusherQueueAutoDelete(queueAutoDelete bool) PusherOption {
	return func(c *Pusher) {
		c.queueAutoDelete = queueAutoDelete
	}
}

func WithPusherQueueExclusive(queueExclusive bool) PusherOption {
	return func(c *Pusher) {
		c.queueExclusive = queueExclusive
	}
}

func WithPusherQueueNoWait(queueNoWait bool) PusherOption {
	return func(c *Pusher) {
		c.queueNoWait = queueNoWait
	}
}

func WithPusherQueueArgs(queueArgs amqp.Table) PusherOption {
	return func(c *Pusher) {
		c.queueArgs = queueArgs
	}
}

func WithPusherQueueBindKey(queueBindKey string) PusherOption {
	return func(c *Pusher) {
		c.queueBindKey = queueBindKey
	}
}

func WithPusherQueueBindNoWait(queueBindNoWait bool) PusherOption {
	return func(c *Pusher) {
		c.queueBindNoWait = queueBindNoWait
	}
}

func WithPusherQueueBindArgs(queueBindArgs amqp.Table) PusherOption {
	return func(c *Pusher) {
		c.queueBindArgs = queueBindArgs
	}
}

func NewPusher(dsn string, options ...PusherOption) Pusher {
	cleanDsn, queueName := unwrapQueueFromDSN(dsn)

	c := Pusher{
		dsn:          cleanDsn,
		exchangeName: queueName,
		exchangeKind: ExchangeFanout,
		queueName:    queueName,
	}

	for _, option := range options {
		option(&c)
	}

	return c
}

func (c *Pusher) Push(job any) error {
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

	var multiErr *multierror.Error

	body, err := json.Marshal(job)
	if err == nil {
		err = channel.Publish(
			c.exchangeName,
			c.queueBindKey,
			false,
			false,
			amqp.Publishing{ContentType: JsonContentType, Body: body},
		)
		if err != nil {
			multiErr = multierror.Append(multiErr, errors.WithStack(err))
		}
	} else {
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
