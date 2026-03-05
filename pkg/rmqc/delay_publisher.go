package rmqc

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

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

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
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

func (p *DelayPublisher) ensureChannel() (*amqp.Channel, error) {
	if p.conn != nil && !p.conn.IsClosed() && p.ch != nil {
		return p.ch, nil
	}
	p.closeUnsafe()

	conn, err := amqp.Dial(p.dsn)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("channel: %w", err)
	}

	if err := ch.ExchangeDeclare(p.exchangeName, p.exchangeKind, p.exchangeDurable, false, false, false, p.exchangeArgs); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("exchange declare: %w", err)
	}

	p.conn = conn
	p.ch = ch
	return ch, nil
}

func (p *DelayPublisher) closeUnsafe() {
	if p.ch != nil {
		_ = p.ch.Close()
		p.ch = nil
	}
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

func (p *DelayPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error
	if p.ch != nil {
		if err := p.ch.Close(); err != nil {
			errs = append(errs, err)
		}
		p.ch = nil
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		p.conn = nil
	}
	return errors.Join(errs...)
}

const maxTTLMs = 1<<31 - 1

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
	if delayMs > maxTTLMs {
		delayMs = maxTTLMs
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	ch, err := p.ensureChannel()
	if err != nil {
		return fmt.Errorf("ensure channel: %w", err)
	}

	delayQueueName := p.makeDelayQueueName(originalExchange, originalRoutingKey, delayMs)
	delayRoutingKey := delayQueueName

	ttl := int32(delayMs)
	expiresMs := delayMs + int(p.queueExpireGrace.Milliseconds())
	if expiresMs > maxTTLMs {
		expiresMs = maxTTLMs
	}
	if int32(expiresMs) < ttl+1000 {
		expiresMs = int(ttl) + 1000
		if expiresMs > maxTTLMs {
			expiresMs = maxTTLMs
		}
	}
	expires := int32(expiresMs)

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
		p.closeUnsafe()
		return fmt.Errorf("queue declare: %w", err)
	}

	err = ch.QueueBind(delayQueueName, delayRoutingKey, p.exchangeName, false, nil)
	if err != nil {
		p.closeUnsafe()
		return fmt.Errorf("queue bind: %w", err)
	}

	pub := amqp.Publishing{ContentType: "application/json", Body: body, Headers: headers, Timestamp: time.Now()}

	err = ch.PublishWithContext(ctx, p.exchangeName, delayRoutingKey, false, false, pub)
	if err != nil {
		p.closeUnsafe()
		return fmt.Errorf("publish: %w", err)
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
