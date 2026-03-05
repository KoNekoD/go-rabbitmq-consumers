package rmqc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

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

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
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

func NewPusher(dsn string, options ...PusherOption) (*Pusher, error) {
	cleanDsn, queueName, err := unwrapQueueFromDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("new pusher: %w", err)
	}

	c := &Pusher{
		dsn:          cleanDsn,
		exchangeName: queueName,
		exchangeKind: ExchangeFanout,
		queueName:    queueName,
	}

	for _, option := range options {
		option(c)
	}

	return c, nil
}

func (c *Pusher) ensureChannel() (*amqp.Channel, error) {
	if c.conn != nil && !c.conn.IsClosed() && c.ch != nil {
		return c.ch, nil
	}
	c.closeUnsafe()

	conn, err := amqp.Dial(c.dsn)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		c.exchangeName,
		string(c.exchangeKind),
		c.exchangeDurable,
		c.exchangeAutoDelete,
		c.exchangeInternal,
		c.exchangeNoWait,
		c.exchangeArgs,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("exchange declare: %w", err)
	}

	if _, err := ch.QueueDeclare(
		c.queueName,
		c.queueDurable,
		c.queueAutoDelete,
		c.queueExclusive,
		c.queueNoWait,
		c.queueArgs,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("queue declare: %w", err)
	}

	if err := ch.QueueBind(
		c.queueName,
		c.queueBindKey,
		c.exchangeName,
		c.queueBindNoWait,
		c.queueBindArgs,
	); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("queue bind: %w", err)
	}

	c.conn = conn
	c.ch = ch
	return ch, nil
}

func (c *Pusher) closeUnsafe() {
	if c.ch != nil {
		_ = c.ch.Close()
		c.ch = nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *Pusher) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	if c.ch != nil {
		if err := c.ch.Close(); err != nil {
			errs = append(errs, err)
		}
		c.ch = nil
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		c.conn = nil
	}
	return errors.Join(errs...)
}

func (c *Pusher) Push(ctx context.Context, job any) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	ch, err := c.ensureChannel()
	if err != nil {
		return fmt.Errorf("ensure channel: %w", err)
	}

	err = ch.PublishWithContext(
		ctx,
		c.exchangeName,
		c.queueBindKey,
		false,
		false,
		amqp.Publishing{ContentType: JsonContentType, Body: body},
	)
	if err != nil {
		c.closeUnsafe()
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
