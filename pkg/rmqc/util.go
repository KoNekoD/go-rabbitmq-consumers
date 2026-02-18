package rmqc

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"strconv"
	"strings"
	"time"
)

// unwrapQueueFromDSN Unwrap queue name from dsn
//
//	If dsn is "amqp://user:pass@localhost:5672/vhost/queueName"
//	It will return dsn "amqp://user:pass@localhost:5672/vhost/" and queueName "queueName"
func unwrapQueueFromDSN(dsn string) (cleanDsn, queueName string) {
	splitDsn := strings.Split(dsn, "/")

	newDsn := strings.Builder{}

	for i, s := range splitDsn {
		// If its last "five" element - it is queue name
		if i == 4 && len(splitDsn) == 5 {
			queueName = s
			break
		}

		newDsn.WriteString(s)
		newDsn.WriteString("/")
	}

	dsn = strings.TrimRight(newDsn.String(), "/")

	return dsn, queueName
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
