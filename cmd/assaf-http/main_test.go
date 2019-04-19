package main

import (
	"encoding/json"
	"fmt"
	"github.com/assaf/assaf-http-server/models"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestRouterHttpHandler(t *testing.T) {
	testRabbitConf(t)
	testParseURI(t)
	testTimeDecay(t)
}

func BenchmarkRouterHttpHandler(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	for n := 0; n < b.N; n++ {
		testRabbitConf(b)
	}
}

func testRabbitConf(t assert.TestingT) {
	target := "amqp://user:user@localhost:3333/"
	os.Setenv("rabbitmq_username", "user")
	os.Setenv("rabbitmq_password", "user")
	os.Setenv("rabbitmq_host", "localhost")
	os.Setenv("rabbitmq_port", "3333")

	assert.Equal(t, target, fmt.Sprint(getRabbitConf()))
}

func testTimeDecay(t assert.TestingT) {
	when := time.Now()
	//t.Errorf("Hello!")
	fresh := msgIsFresh(models.ConsumeRequest{1, make(chan models.Message), when.Add(-10 * time.Second)})
	assert.Equal(t, true, fresh)
	fresh = msgIsFresh(models.ConsumeRequest{1, make(chan models.Message), when})
	assert.Equal(t, false, fresh)

}
func testParseURI(t assert.TestingT) {
	p_new_style := "Php+Developer"
	p_old_style := "Php%20Developer"

	lang := "nl"

	q := httptest.NewRequest("GET", fmt.Sprintf("http://example.com/google-keywords/%s?job_category=Sales&location=%s&location_id=2528&language_id=1010", p_new_style, lang), nil)

	fmt.Printf("q request uri: %v", q.RequestURI)

	gkw, _ := buildQuery(q.URL)
	gkwM, _ := json.Marshal(gkw.values)
	fmt.Printf("path: %v, routing: %v, marshaled: %v", gkw.p, gkw.routing, string(gkwM))
	assert.Equal(t, lang, gkw.values.Get("location"))
	cp, _ := cleanRequest(gkw.p)
	cp2, _ := cleanRequest(p_old_style)
	correct := "Php Developer"
	assert.Equal(t, correct, cp)
	assert.Equal(t, correct, cp2)

}
