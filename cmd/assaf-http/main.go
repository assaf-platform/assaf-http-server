// This example declares a durable Exchange, and publishes a single message to
// that Exchange with a given routing key.
//
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/opentracing-contrib/go-amqp/amqptracer"
	"github.com/opentracing/opentracing-go"
	"github.com/streadway/amqp"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	//_uri          = flag.String("uri", "amqp://guest:guest@localhost:32769/", "AMQP URI")
	_exchangeName = flag.String("exchange", "ds-exchange", "Durable AMQP exchange name")
	_exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	//_routingKey   = flag.String("key", "acrit", "AMQP routing key")
	//_body         = flag.String("body", `{"profile":"PHP Developer"}`, "Body of message")
	//_reliable     = flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")
)

// initJaeger returns an instance of Jaeger Tracer that samples 100% of traces and logs all spans to stdout.
func initJaeger(service string) (opentracing.Tracer, io.Closer) {
	cfg := &config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LogSpans: true,
		},
		ServiceName: service,
	}

	log.Print("#######################")
	log.Print(cfg.Sampler.SamplingServerURL)

	tracer, closer, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	return tracer, closer
}

func Logger(inner http.Handler, name string) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		start := time.Now()

		inner.ServeHTTP(w, r)

		log.Printf(
			"%s\t%s\t%s\t%s",
			r.Method,
			r.RequestURI,
			name,
			time.Since(start),
		)
	})
}

func RabbitConnect(amqpURI string) (session, error) {
	nilSession := session{nil, nil, "error", "error"}
	log.Printf("dialing %q", amqpURI)
	connection, err := amqp.Dial(amqpURI)
	if err != nil {
		return nilSession, fmt.Errorf("Dial: %s", err)
	}

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		return nilSession, fmt.Errorf("Channel: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.

	log.Printf("enabling publishing confirms.")
	if err := channel.Confirm(false); err != nil {
		return nilSession, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}
	//
	//confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	//
	//defer confirmOne(confirms)

	return session{connection, channel, "", ""}, nil
}

func declareExchange(channel *amqp.Channel, exchangeName string, exchangeType string, isDurable bool, isAutoDelete bool) error {
	log.Printf("got Channel, declaring %q Exchange (%q)", exchangeType, exchangeName)

	if err := channel.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		isDurable,    // durable
		isAutoDelete, // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	return nil
}

// publish publishes messages to a reconnecting session to a fanout exchange.
// It receives from the application specific source of messages.

func publishToQueue(sessions chan chan session, msgs <-chan message, tracer opentracing.Tracer) error {

	// This function dials, connects, declares, publishes, and tears down,
	// all in one go. In a real service, you probably want to maintain a
	// long-lived connection as state, and publish against that.

	for session := range sessions {
		pubSesh := <-session
		for msg := range msgs {

			spo := opentracing.ChildOf(msg.spanCtx)
			sp := tracer.StartSpan("publishMessage", spo)

			amqpMsg := amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "application/json",
				ContentEncoding: "",
				Body:            msg.body,
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
				// a bunch of application/implementation-specific fields
			}
			// Inject the span context into the AMQP header.
			if err := amqptracer.Inject(sp, amqpMsg.Headers); err != nil {
				log.Fatal("amqp tracer couldn't inject message to headers", err)

			}
			log.Printf("publishing %dB body (%q), routing: %s", len(msg.body), msg.body, msg.routingKey)

			pubErr := pubSesh.Channel.Publish(
				pubSesh.exchangeName, // publish to an exchange
				msg.routingKey,       // routing to 0 or more queues
				false,                // mandatory
				false,                // immediate
				amqpMsg,
			)

			if pubErr != nil {
				return fmt.Errorf("exchange publish error: %s", pubErr)
			}
			sp.Finish()
		}
	}

	return nil
}

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}

// A message going over the wire
type DataRequest struct {
	Body      string     `json:"body"`
	RequestID int        `json:"request-id"`
	Options   url.Values `json:"options"`
}

// A request message connecting between the http endpoint and the
// async nature of rabbitmq
type ConsumeRequest struct {
	reqId      int
	reqChannel chan<- message
	timestamp  time.Time
}

// message is the application type for a message.  This can contain identity,
// or a reference to the recevier chan for further demuxing.
type message struct {
	body       []byte
	routingKey string
	spanCtx    opentracing.SpanContext
}

// session composes an amqp.Connection with an amqp.Channel
type session struct {
	*amqp.Connection
	*amqp.Channel
	exchangeName string
	exchangeType string
}

// Close tears the connection down, taking the channel with it.
func (s session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

// redial continually connects to the URL, exiting the program when no longer possible
func redial(ctx context.Context, url string, exchangeName string, exchangeType string, isDurable bool, isAutoDelete bool) chan chan session {
	sessions := make(chan chan session)

	go func() {
		sess := make(chan session)
		defer close(sessions)
		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				log.Println("shutting down session factory")
				return
			}

			partialSession, err := RabbitConnect(url)
			if err != nil {
				log.Fatalf("couldn't create connection %v", partialSession)
			}
			err = declareExchange(partialSession.Channel, exchangeName, exchangeType, isDurable, isAutoDelete)
			if err != nil {
				log.Fatalf("couldn't create exchange %v", err)
			}
			log.Printf("session created in redial %v", partialSession)
			currSession := session{partialSession.Connection, partialSession.Channel, exchangeName, exchangeType}
			if err != nil {
				log.Fatalf("Channel could not be put into confirm mode: %s", err)
			}

			select {
			case sess <- currSession:
			case <-ctx.Done():
				log.Println("shutting down new session")
				currSession.Close()
			}
		}
	}()

	return sessions
}

var REQUSET_TIMEOUT = 10.0

func msgIsFresh(cm ConsumeRequest) bool {
	return (time.Now().Unix() - cm.timestamp.Unix()) <= int64(REQUSET_TIMEOUT)

}

// publish publishes messages to a reconnecting session to a fanout exchange.
// It receives from the application specific source of messages.

// identity returns the same host/process unique string for the lifetime of
// this process so that subscriber reconnections reuse the same queue name.

func subscribe(queueName string, sessions chan chan session, consumeRequests chan ConsumeRequest, tracer opentracing.Tracer) {

	for session := range sessions {
		sub := <-session

		log.Printf("got session with exchange %s in subscribe", sub.exchangeName)
		if _, err := sub.QueueDeclare(queueName, false, true, false, false, nil); err != nil {
			log.Printf("cannot consume from queue: %q, %v", queueName, err)
			return
		}
		//for consumeRequest := range consumeRequests {
		bindKey := "#"
		log.Println("subscriber binding to:", bindKey)
		if err := sub.QueueBind(queueName, bindKey, sub.exchangeName, false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", sub.exchangeName, err)
			return
		}

		deliveries, err := sub.Consume(queueName, fmt.Sprintf("%v", session), false, false, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", queueName, err)
			return
		}

		log.Printf("subscribed...")

		defaultHeaders := make(map[string]interface{})
		defaultHeaders["uber-trace-id"] = "stam"
		for msg := range deliveries {
			routingKey, cerr := strconv.Atoi(msg.RoutingKey)
			if cerr != nil {
				log.Println("routing key was nil", cerr)
				break
			}
			log.Printf("got msg in deliveries! %v", msg.Body)
			log.Printf("routing key: %d", routingKey)
			go func() {
				for consumeRequest := range consumeRequests {
					if consumeRequest.reqId == routingKey {
						headers := msg.Headers
						if msg.Headers["uber-trace-id"] == nil {
							headers = defaultHeaders
						}
						carrier := opentracing.TextMapCarrier{"uber-trace-id": headers["uber-trace-id"].(string)}
						spCtx, terr := tracer.Extract(opentracing.TextMap, carrier)
						if terr != nil {
							log.Printf("something went wrong %v", terr)
						}

						if spCtx == nil {
							spCtx = tracer.StartSpan("span").Context()
						}

						sp := tracer.StartSpan(
							"ReturnToClient",
							opentracing.ChildOf(spCtx),
						)
						msg.Ack(false)
						consumeRequest.reqChannel <- message{msg.Body, msg.RoutingKey, sp.Context()}
						sp.Finish()
						break
					} else if msgIsFresh(consumeRequest) {
						consumeRequests <- consumeRequest
					} else {
						log.Println("messages is stale")
						log.Println(consumeRequest.timestamp)
						log.Println("time.Since(cm.timestamp).Seconds()")
						log.Println(time.Since(consumeRequest.timestamp).Seconds())
						msg.Ack(false)
					}
				}
			}()
		}
	}
}

func buildQuery(u *url.URL) (CleanQuery, error) {
	p, err := url.ParseRequestURI(u.RequestURI())
	blank := CleanQuery{"", "", p.Query()}
	fmt.Printf("%v", p)
	if err != nil {
		return blank, err
	}

	log.Printf("got query %s", p)

	cleanQuery := strings.Split(p.Path, "/")
	if len(cleanQuery) < 3 {
		return blank, url.EscapeError("bad request, not enough arguments")
	}
	return CleanQuery{routing: cleanQuery[1], p: cleanQuery[2], values: p.Query()}, nil
}
func cleanRequest(p string) (string, error) {

	cleanPath, err := url.QueryUnescape(p)

	if err != nil {
		log.Printf("error %s", err)
		return "", err
	}
	return cleanPath, nil
}

type CleanQuery struct {
	routing string
	p       string
	values  url.Values
}

func requestHandler(consumeRequests chan ConsumeRequest, msgQueue chan<- message, tracer opentracing.Tracer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := tracer.StartSpan("call-http")
		defer span.Finish()
		rand.Seed(time.Now().Unix())
		requestId := rand.Intn(math.MaxInt8)
		cq, err := buildQuery(r.URL)
		fmt.Printf("%v", cq)
		if err != nil {
			fmt.Fprintf(w, "error: %s", err)
			log.Printf("error %s", err)
			return
		}

		cleanPath, err := cleanRequest(cq.p)

		if err != nil {
			fmt.Fprintf(w, "error: %s", err)
			log.Printf("error %s", err)
			return
		}

		routing := strings.Replace(cq.routing, "-", "", -1)
		request := DataRequest{cleanPath, requestId, cq.values}
		subscribe := make(chan message)
		defer close(subscribe)
		jsonRequest, err := json.Marshal(request)
		if err != nil {
			fmt.Fprintf(w, "error: %s", err)
			log.Printf("error %s", err)
			return
		}
		msgQueue <- message{jsonRequest, routing, span.Context()}
		log.Println("###  before consume requests!")
		consumeRequests <- ConsumeRequest{reqId: requestId, reqChannel: subscribe, timestamp: time.Now()}
		log.Println("before select!")
		select {
		case res := <-subscribe:
			log.Printf("in controller length: %d, 1st: %s, 2nd %s", len(res.body), res.body, res.routingKey)
			w.Header().Set("Content-Type", "application/json")
			// we are sending the second part of res since the first part is the id
			fmt.Fprintf(w, "%s", res.body)
			//close(subscribe)
		case <-time.After(time.Duration(REQUSET_TIMEOUT) * time.Second):
			fmt.Printf("timeout after %f seconds \n", REQUSET_TIMEOUT)
			w.WriteHeader(408)
			//close(subscribe)
		}

	})
}

func getConf(key string) string {
	//namespace := ""
	return os.Getenv(key)
}

type RabbitMQConf struct {
	user string
	pass string
	host string
	port int
}

func getRabbitConf() RabbitMQConf {
	port, _ := strconv.Atoi(getConf("rabbitmq_port"))
	return RabbitMQConf{
		getConf("rabbitmq_username"),
		getConf("rabbitmq_password"),
		getConf("rabbitmq_host"),
		port}
}

func (rc RabbitMQConf) String() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/", rc.user, rc.pass, rc.host, rc.port)
}

func main() {

	tracer, closer := initJaeger("assaf-frontend")
	defer closer.Close()

	url := fmt.Sprint(getRabbitConf())

	ctx, done := context.WithCancel(context.Background())

	msgSessions := redial(ctx, url, *_exchangeName, *_exchangeType, true, false)
	pubsubSessions := redial(ctx, url, "pubsub", "topic", false, true)
	toQueue := make(chan message)
	consumeRequests := make(chan ConsumeRequest)

	go publishToQueue(msgSessions, toQueue, tracer)

	go subscribe("response_pubsub", pubsubSessions, consumeRequests, tracer)

	defer done()

	routerHttpHandler := Logger(requestHandler(consumeRequests, toQueue, tracer), "ds-proxy")
	log.Fatal(http.ListenAndServe(":8080", routerHttpHandler))

	//<-ctx.Done()
}
