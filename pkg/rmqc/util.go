package rmqc

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// unwrapQueueFromDSN Unwrap queue name from dsn
//
//	If dsn is "amqp://user:pass@localhost:5672/vhost/queueName"
//	It will return dsn "amqp://user:pass@localhost:5672/vhost" and queueName "queueName"
func unwrapQueueFromDSN(dsn string) (cleanDsn, queueName string, err error) {
	splitDsn := strings.Split(dsn, "/")
	if len(splitDsn) != 5 {
		return "", "", fmt.Errorf("rmqc: invalid DSN format: expected 5 path segments, got %d", len(splitDsn))
	}

	queueName = splitDsn[4]
	if queueName == "" {
		return "", "", fmt.Errorf("rmqc: empty queue name in DSN")
	}

	cleanDsn = strings.Join(splitDsn[:4], "/")

	return cleanDsn, queueName, nil
}

const retryAttemptHeader = "x-retry-attempt"

func getRetryAttempt(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	v, ok := headers[retryAttemptHeader]
	if !ok || v == nil {
		return 0
	}

	switch t := v.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case float32:
		return int(t)
	case float64:
		return int(t)
	case string:
		if n, err := strconv.Atoi(t); err == nil {
			return n
		}
	}
	return 0
}

func setRetryAttempt(headers amqp.Table, attempt int) amqp.Table {
	if headers == nil {
		headers = amqp.Table{}
	}
	headers[retryAttemptHeader] = attempt
	return headers
}

func exponentialBackoffDelay(attempt int, baseDelay, maxDelay time.Duration) time.Duration {
	if attempt <= 1 {
		if baseDelay > maxDelay {
			return maxDelay
		}
		return baseDelay
	}

	d := baseDelay
	shift := attempt - 1
	for i := 0; i < shift; i++ {
		if d >= maxDelay/2 {
			return maxDelay
		}
		d *= 2
	}
	if d > maxDelay {
		return maxDelay
	}
	return d
}
