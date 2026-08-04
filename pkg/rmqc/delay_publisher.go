package rmqc

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"

	"go.uber.org/zap"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type DelayPublisher struct {
	dsn string

	exchangeName string
	exchangeKind string

	queueExpireGrace time.Duration

	queueArgs amqp.Table

	exchangeArgs amqp.Table

	exchangeDurable bool
	queueDurable    bool

	logger *zap.Logger
}

type DelayPublisherOption func(*DelayPublisher)

func NewDelayPublisher(dsn string, opts ...DelayPublisherOption) *DelayPublisher {
	p := &DelayPublisher{
		dsn:              dsn,
		exchangeName:     "delays",
		exchangeKind:     "direct",
		queueExpireGrace: 10 * time.Second,
		queueArgs:        amqp.Table{"x-queue-type": "classic"},
		exchangeArgs:     nil,
		exchangeDurable:  true,
		queueDurable:     true,
		logger:           zap.Must(zap.NewDevelopment()),
	}

	for _, o := range opts {
		o(p)
	}

	return p
}

func WithDelayExchangeName(name string) DelayPublisherOption {
	return func(p *DelayPublisher) { p.exchangeName = name }
}

func WithQueueExpireGrace(d time.Duration) DelayPublisherOption {
	return func(p *DelayPublisher) { p.queueExpireGrace = d }
}

func WithDelayQueueArgs(args amqp.Table) DelayPublisherOption {
	return func(p *DelayPublisher) {
		if p.queueArgs == nil {
			p.queueArgs = amqp.Table{}
		}
		for k, v := range args {
			p.queueArgs[k] = v
		}
	}
}

func WithDelayExchangeArgs(args amqp.Table) DelayPublisherOption {
	return func(p *DelayPublisher) { p.exchangeArgs = args }
}

func WithDelayExchangeDurable(v bool) DelayPublisherOption {
	return func(p *DelayPublisher) { p.exchangeDurable = v }
}

func WithDelayQueueDurable(v bool) DelayPublisherOption {
	return func(p *DelayPublisher) { p.queueDurable = v }
}

func WithDelayLogger(logger *zap.Logger) DelayPublisherOption {
	return func(p *DelayPublisher) { p.logger = logger }
}

func (p *DelayPublisher) PublishDelayedJSON(
	originalExchange string,
	originalRoutingKey string,
	body []byte,
	delayMs int32,
	headers amqp.Table,
) error {
	if delayMs < 0 {
		delayMs = 0
	}

	connection, err := amqp.Dial(p.dsn)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			p.logger.Error("failed to close connection", zap.Error(err))
		}
	}()

	channel, err := connection.Channel()
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		if err := channel.Close(); err != nil {
			p.logger.Error("failed to close channel", zap.Error(err))
		}
	}()

	err = channel.ExchangeDeclare(
		p.exchangeName,
		p.exchangeKind,
		p.exchangeDurable,
		false,
		false,
		false,
		p.exchangeArgs,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	delayQueueName := p.makeDelayQueueName(originalExchange, originalRoutingKey, delayMs)
	delayRoutingKey := delayQueueName

	ttl := delayMs
	expires := delayMs + int32(p.queueExpireGrace.Milliseconds())
	if expires < ttl+1000 {
		expires = ttl + 1000
	}

	args := make(amqp.Table, len(p.queueArgs)+4)
	for k, v := range p.queueArgs {
		args[k] = v
	}
	args["x-message-ttl"] = ttl
	args["x-dead-letter-exchange"] = originalExchange
	args["x-dead-letter-routing-key"] = originalRoutingKey
	args["x-expires"] = expires

	_, err = channel.QueueDeclare(delayQueueName, p.queueDurable, false, false, false, args)
	if err != nil {
		return errors.WithStack(err)
	}

	err = channel.QueueBind(delayQueueName, delayRoutingKey, p.exchangeName, false, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	err = publish(channel, body, p.exchangeName, delayRoutingKey, headers)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

var nonSafe = regexp.MustCompile(`[^a-zA-Z0-9_.:-]+`)

func (p *DelayPublisher) makeDelayQueueName(exchangeName, routingKey string, delayMs int32) string {
	name := fmt.Sprintf("delay_%s_%s_%d_delay", exchangeName, routingKey, delayMs)

	safe := nonSafe.ReplaceAllString(name, "_")
	if len(safe) <= 230 {
		return safe
	}

	h := sha1.Sum([]byte(name))

	return "delay_" + hex.EncodeToString(h[:])[:32] + fmt.Sprintf("_%d", delayMs) + "_delay"
}
