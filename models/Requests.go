package models

import (
	"github.com/opentracing/opentracing-go"
	"github.com/streadway/amqp"
	"net/url"
	"time"
)

// A Message going over the wire
type DataRequest struct {
	Body      string     `json:"body"`
	RequestID int        `json:"request-id"`
	Options   url.Values `json:"options"`
}

// Close tears the connection down, taking the channel with it.
func (s Session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

// A request Message connecting between the http endpoint and the
// async nature of rabbitmq
type ConsumeRequest struct {
	ReqId      int
	ReqChannel chan Message
	Timestamp  time.Time
}

// Message is the application type for a Message.  This can contain identity,
// or a reference to the recevier chan for further demuxing.
type Message struct {
	Body       []byte
	RoutingKey string
	SpanCtx    opentracing.SpanContext
	HttpStatus int32
}

// Session composes an amqp.Connection with an amqp.Channel
type Session struct {
	*amqp.Connection
	*amqp.Channel
	ExchangeName string
	ExchangeType string
}
