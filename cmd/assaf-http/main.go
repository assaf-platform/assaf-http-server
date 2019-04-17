// This example declares a durable Exchange, and publishes a single Message to
// that Exchange with a given routing key.
//
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/assaf/assaf-http-server/models"
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
	"sync"
	"time"
)

var (
	//_uri          = flag.String("uri", "amqp://guest:guest@localhost:32769/", "AMQP URI")
	_exchangeName = flag.String("exchange", "ds-exchange", "Durable AMQP exchange name")
	_exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	//_routingKey   = flag.String("key", "acrit", "AMQP routing key")
	//_body         = flag.String("Body", `{"profile":"PHP Developer"}`, "Body of Message")
	//_reliable     = flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")
)

// initJaeger returns an instance of Jaeger Tracer that samples 100% of traces and logs all spans to stdout.
func initJaeger(service string) (opentracing.Tracer, io.Closer) {
	//cfg := &config.Configuration{
	//	Sampler: &config.SamplerConfig{
	//		Type:  "const",
	//		Param: 1,
	//	},
	//	Reporter: &config.ReporterConfig{
	//		LogSpans: true,
	//	},
	//	ServiceName: service,
	//}
	cfg, err := config.FromEnv()
	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}
	cfg.Sampler.Type = "const"
	cfg.Sampler.Param = 1
	cfg.ServiceName = service
	cfg.Reporter.LogSpans = true
	log.Print("#######################")
	log.Print(cfg.Reporter.LocalAgentHostPort)

	tracer, closer, err := cfg.NewTracer(config.Logger(jaeger.StdLogger))
	if err != nil {
		log.Panic("can not init jaeger %v", err)
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

func RabbitConnect(amqpURI string) (models.Session, error) {
	nilSession := models.Session{nil, nil, "error", "error"}
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

	return models.Session{connection, channel, "", ""}, nil
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

// publish publishes messages to a reconnecting Session to a fanout exchange.
// It receives from the application specific source of messages.

func publishToQueue(sessions chan chan models.Session, msgs <-chan models.Message, tracer opentracing.Tracer) error {

	// This function dials, connects, declares, publishes, and tears down,
	// all in one go. In a real service, you probably want to maintain a
	// long-lived connection as state, and publish against that.

	for session := range sessions {
		pubSesh := <-session
		for msg := range msgs {

			spo := opentracing.ChildOf(msg.SpanCtx)
			sp := tracer.StartSpan("publishMessage", spo)

			amqpMsg := amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "application/json",
				ContentEncoding: "",
				Body:            msg.Body,
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
				// a bunch of application/implementation-specific fields
			}
			// Inject the span context into the AMQP header.
			if err := amqptracer.Inject(sp, amqpMsg.Headers); err != nil {
				log.Fatal("amqp tracer couldn't inject Message to headers", err)

			}
			log.Printf("publishing %dB Body (%q), routing: %s", len(msg.Body), msg.Body, msg.RoutingKey)

			pubErr := pubSesh.Channel.Publish(
				pubSesh.ExchangeName, // publish to an exchange
				msg.RoutingKey,       // routing to 0 or more queues
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

// redial continually connects to the URL, exiting the program when no longer possible
func redial(ctx context.Context, url string, exchangeName string, exchangeType string, isDurable bool, isAutoDelete bool) chan chan models.Session {
	sessions := make(chan chan models.Session)

	go func() {
		sess := make(chan models.Session)
		defer close(sessions)
		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				log.Println("shutting down Session factory")
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
			log.Printf("Session created in redial %v", partialSession)

			currSession := models.Session{partialSession.Connection, partialSession.Channel, exchangeName, exchangeType}
			if err != nil {
				log.Fatalf("Channel could not be put into confirm mode: %s", err)
			}

			select {
			case sess <- currSession:
			case <-ctx.Done():
				log.Println("shutting down new Session")
				currSession.Close()
			}
		}
	}()

	return sessions
}

var REQUSET_TIMEOUT = 60.0

func msgIsFresh(cm models.ConsumeRequest) bool {
	return (time.Now().Unix() - cm.Timestamp.Unix()) <= int64(REQUSET_TIMEOUT)

}

// publish publishes messages to a reconnecting Session to a fanout exchange.
// It receives from the application specific source of messages.

// identity returns the same host/process unique string for the lifetime of
// this process so that subscriber reconnections reuse the same queue name.

func subscribe(queueName string, sessions chan chan models.Session, tracer opentracing.Tracer, requests *Reqs) {

	for session := range sessions {
		sub := <-session

		log.Printf("got Session with exchange %s in subscribe", sub.ExchangeName)
		if _, err := sub.QueueDeclare(queueName, false, true, false, false, nil); err != nil {
			log.Printf("cannot consume from queue: %q, %v", queueName, err)
			return
		}
		//for consumeRequest := range consumeRequests {
		bindKey := "#"
		log.Println("subscriber binding to:", bindKey)
		if err := sub.QueueBind(queueName, bindKey, sub.ExchangeName, false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", sub.ExchangeName, err)
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
		for n_msg := range deliveries {
			go returnResponse(n_msg, requests, defaultHeaders, tracer)
		}
	}
}

func returnResponse(c_msg amqp.Delivery, requests *Reqs, defaultHeaders map[string]interface{}, tracer opentracing.Tracer) error {
	routingKey, cerr := strconv.Atoi(c_msg.RoutingKey)
	if cerr != nil {
		log.Println("routing key was nil", cerr)
	}

	consumeRequest, ok := requests.Load(routingKey)

	if ok == false {
		return errors.New("request not found")
	}
	headers := c_msg.Headers
	if c_msg.Headers["uber-trace-id"] == nil {
		headers = defaultHeaders
	}
	carrier := opentracing.TextMapCarrier{"uber-trace-id": headers["uber-trace-id"].(string)}
	spCtx, terr := tracer.Extract(opentracing.TextMap, carrier)
	if terr != nil {
		return errors.New("no trace to extract")
	}

	if spCtx == nil {
		spCtx = tracer.StartSpan("span").Context()
	}

	sp := tracer.StartSpan(
		"ReturnToClient",
		opentracing.ChildOf(spCtx),
	)
	c_msg.Ack(false)
	log.Printf("got request id: %d", consumeRequest.ReqId)
	select {
	case <-consumeRequest.ReqChannel:
		print("!!!!! channel already closed")
		return errors.New("request closed before response")
	case consumeRequest.ReqChannel <- models.Message{Body: c_msg.Body, RoutingKey: c_msg.RoutingKey, SpanCtx: sp.Context()}:
	}

	sp.Finish()
	return nil
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

func requestHandler(msgQueue chan<- models.Message, tracer opentracing.Tracer, requests *Reqs) http.Handler {
	rand.Seed(time.Now().Unix())
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		span := tracer.StartSpan("call-http")
		defer span.Finish()
		requestId := rand.Intn(math.MaxInt32)
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
		request := models.DataRequest{cleanPath, requestId, cq.values}
		jsonRequest, err := json.Marshal(request)
		if err != nil {
			fmt.Fprintf(w, "error: %s", err)
			log.Printf("error %s", err)
			return
		}
		subscribe := make(chan models.Message)
		// if this request returns the done channel should
		// complete before the subscribe channel defending us from
		// send on closed channel panics
		defer close(subscribe)

		msgQueue <- models.Message{jsonRequest, routing, span.Context()}

		requests.Store(requestId, models.ConsumeRequest{ReqId: requestId, ReqChannel: subscribe, Timestamp: time.Now()})
		defer requests.Delete(requestId)
		log.Println("before select!")
		select {
		case res := <-subscribe:
			log.Printf("in controller length: %d, routing key: %s, request uri: %v", len(res.Body), res.RoutingKey, r.RequestURI)
			w.Header().Set("Content-Type", "application/json")
			// we are sending the second part of res since the first part is the id
			fmt.Fprintf(w, "%s", res.Body)
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

type Reqs struct {
	sync.RWMutex
	internal map[int]models.ConsumeRequest
}

func NewReqs() *Reqs {
	return &Reqs{
		internal: make(map[int]models.ConsumeRequest),
	}
}

func (rm *Reqs) Load(key int) (value models.ConsumeRequest, ok bool) {
	rm.RLock()
	result, ok := rm.internal[key]
	rm.RUnlock()
	return result, ok
}

func (rm *Reqs) Delete(key int) {
	rm.Lock()
	delete(rm.internal, key)
	rm.Unlock()
}

func (rm *Reqs) Store(key int, value models.ConsumeRequest) {
	rm.Lock()
	rm.internal[key] = value
	rm.Unlock()
}
func main() {

	tracer, closer := initJaeger("assaf-frontend")
	defer closer.Close()

	rabbit_url := fmt.Sprint(getRabbitConf())

	ctx, done := context.WithCancel(context.Background())

	msgSessions := redial(ctx, rabbit_url, *_exchangeName, *_exchangeType, true, false)
	pubsubSessions := redial(ctx, rabbit_url, "pubsub", "topic", false, true)
	toQueue := make(chan models.Message)

	requests := NewReqs()

	go publishToQueue(msgSessions, toQueue, tracer)

	go subscribe("response_pubsub", pubsubSessions, tracer, requests)

	defer done()

	routerHttpHandler := Logger(requestHandler(toQueue, tracer, requests), "ds-proxy")
	log.Fatal(http.ListenAndServe(":8080", routerHttpHandler))

	//<-ctx.Done()
}
