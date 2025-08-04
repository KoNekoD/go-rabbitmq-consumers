# Go RabbitMQ Consumers Helpers

A set of tools for creating RabbitMQ consumers and message publishers

## Usage

```go
package consumers

import (
	"context"
	"github.com/KoNekoD/go-rabbitmq-consumers/pkg/rmqc"
	"github.com/pkg/errors"
	"time"
)

const exampleDsn = "amqp://guest:guest@localhost:5672/example"

type ExampleJob struct {
	ID     int
	UserId int
}

type ExampleJobPublisher struct {
	pusher rmqc.Pusher
}

func NewExampleJobPublisher() *ExampleJobPublisher {
	pusher := rmqc.NewPusher(exampleDsn)

	return &ExampleJobPublisher{pusher: pusher}
}

func (p *ExampleJobPublisher) Publish(id int) error {
	job := ExampleJob{
		ID:     id,
		UserId: 111,
	}

	if err := p.pusher.Push(job); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

type ExampleConsumer struct {
	rmqc.AbstractConsumer[ExampleJob]
}

func NewExampleConsumer() *ExampleConsumer {
	dsn := exampleDsn

	c := &ExampleConsumer{
		AbstractConsumer: rmqc.NewAbstractConsumer[ExampleJob](dsn),
	}

	return rmqc.SetupConsumerChild(&c.AbstractConsumer, c)
}

func (c *ExampleConsumer) Handle(job ExampleJob) error {
	// Every job must be handled in hour
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	_ = ctx
	// Action logic goes here

	return nil
}
```

```go
package main

import (
	"my_project/pkg/consumers"
	"github.com/charmbracelet/log"
)

func main() {
	consumer := consumers.NewExampleConsumer()

	err := consumer.Init()
	if err != nil {
		log.Fatal("Example consumer error", "err", err)
	}
}

```
