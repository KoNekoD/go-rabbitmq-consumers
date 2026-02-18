package rmqc

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

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

func (p *DelayPublisher) PublishDelayedJSON(
	ctx context.Context,
	originalExchange string,
	originalRoutingKey string,
	payload any,
	delayMs int,
	headers amqp.Table,
) error {
	if delayMs < 0 {
		delayMs = 0
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return errors.WithStack(err)
	}

	conn, err := amqp.Dial(p.dsn)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = ch.Close() }()

	err = ch.ExchangeDeclare(p.exchangeName, p.exchangeKind, p.exchangeDurable, false, false, false, p.exchangeArgs)
	if err != nil {
		return errors.WithStack(err)
	}

	delayQueueName := p.makeDelayQueueName(originalExchange, originalRoutingKey, delayMs)
	delayRoutingKey := delayQueueName

	ttl := int32(delayMs)
	expires := int32(delayMs + int(p.queueExpireGrace.Milliseconds()))
	if expires < ttl+1000 {
		expires = ttl + 1000
	}

	args := amqp.Table{
		"x-message-ttl":             ttl,
		"x-dead-letter-exchange":    originalExchange,
		"x-dead-letter-routing-key": originalRoutingKey,
		"x-expires":                 expires,
	}
	for k, v := range p.queueArgs {
		args[k] = v
	}

	_, err = ch.QueueDeclare(delayQueueName, p.queueDurable, false, false, false, args)
	if err != nil {
		return errors.WithStack(err)
	}

	err = ch.QueueBind(delayQueueName, delayRoutingKey, p.exchangeName, false, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	pub := amqp.Publishing{ContentType: "application/json", Body: body, Headers: headers, Timestamp: time.Now()}

	err = ch.PublishWithContext(ctx, p.exchangeName, delayRoutingKey, false, false, pub)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

var nonSafe = regexp.MustCompile(`[^a-zA-Z0-9_.:-]+`)

func (p *DelayPublisher) makeDelayQueueName(exchangeName, routingKey string, delayMs int) string {
	name := fmt.Sprintf("delay_%s_%s_%d_delay", exchangeName, routingKey, delayMs)

	safe := nonSafe.ReplaceAllString(name, "_")
	if len(safe) <= 230 {
		return safe
	}

	h := sha1.Sum([]byte(name))

	return "delay_" + hex.EncodeToString(h[:])[:32] + fmt.Sprintf("_%d", delayMs) + "_delay"
}
