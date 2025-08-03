package rmqc

import "strings"

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
