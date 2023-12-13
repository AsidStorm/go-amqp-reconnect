package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rabbitmq.Debug = true

	url := flag.String("url", "amqp://127.0.0.1:5672/", "amqp://user:password@host:port/")
	flag.Parse()

	conn, err := rabbitmq.Dial(*url)
	if err != nil {
		panic(err)
	}

	sendCh, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	consumeCh, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	err = consumeCh.Qos(1, 0, false)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			err := sendCh.Publish("", "test", false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(time.Now().String()),
			})
			log.Printf("publish, err: %v", err)
			time.Sleep(time.Second * 5)
		}
	}()

	go func() {
		d, err := consumeCh.Consume("test", "", false, false, false, false, nil)
		if err != nil {
			log.Panic(err)
		}

		for msg := range d {
			log.Printf("msg: %s", string(msg.Body))
			time.Sleep(time.Second * 2)
			log.Printf("ack, err: %v", msg.Ack(false))
		}

		log.Printf("consumer closed")
	}()

	go func() {
		time.Sleep(time.Second * 5)
		log.Printf("close send channel, err: %v", sendCh.Close())
		log.Printf("close consume channel, err: %v", consumeCh.Close())
		log.Printf("close connection, err: %v", conn.Close())
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}
