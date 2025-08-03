package rmqc

type ExchangeKind string

const (
	ExchangeDirect  ExchangeKind = "direct"
	ExchangeFanout  ExchangeKind = "fanout"
	ExchangeTopic   ExchangeKind = "topic"
	ExchangeHeaders ExchangeKind = "headers"
)
