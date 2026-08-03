package rmqc

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

func publish(channel *amqp.Channel, body []byte, exchange, key string, headers amqp.Table) error {
	if err := channel.Confirm(false); err != nil {
		return errors.WithStack(err)
	}

	returns := channel.NotifyReturn(make(chan amqp.Return, 1))
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	err := channel.Publish(
		exchange,
		key,
		true,
		false,
		amqp.Publishing{
			ContentType:  JsonContentType,
			DeliveryMode: amqp.Persistent,
			Body:         body,
			Timestamp:    time.Now(),
			Headers:      headers,
		},
	)
	if err != nil {
		return errors.WithStack(err)
	}

	confirmation, ok := <-confirms
	if !ok {
		return errors.New("publisher confirmation channel closed")
	}

	if !confirmation.Ack {
		return errors.New("RabbitMQ negatively acknowledged publication")
	}

	select {
	case returned := <-returns:
		return fmt.Errorf(
			"message was not routed: code=%d text=%s exchange=%s routing_key=%s",
			returned.ReplyCode,
			returned.ReplyText,
			returned.Exchange,
			returned.RoutingKey,
		)
	default:
	}

	return nil
}
