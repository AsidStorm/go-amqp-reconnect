package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AsidStorm/go-amqp-reconnect/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rabbitmq.Debug = true

	url := flag.String("url", "amqp://127.0.0.1:5672/", "amqp://user:password@host:port/")
	flag.Parse()

	ctx, closeCtx := context.WithCancel(context.Background())

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

	_, err = consumeCh.QueueDeclare("test-auto-delete", false, true, false, true, nil)
	if err != nil {
		panic(err)
	}

	consumerDone := make(chan bool, 1)
	producerDone := make(chan bool, 1)

	go func() {
		for {
			stop := false

			select {
			case <-ctx.Done():
				stop = true
			default:
				err := sendCh.Publish("", "test-auto-delete", false, false, amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(time.Now().String()),
				})
				log.Printf("publish, err: %v", err)
				time.Sleep(time.Second * 5)
			}

			if stop {
				break
			}
		}

		producerDone <- true
	}()

	go func() {
		d, err := consumeCh.Consume("test-auto-delete", "", false, false, false, false, nil)
		if err != nil {
			log.Panic(err)
		}

		for {
			stop := false

			select {
			case <-ctx.Done():
				stop = true
			case msg := <-d:
				log.Printf("msg: %s", string(msg.Body))
				time.Sleep(time.Second * 2)
				log.Printf("ack, err: %v", msg.Ack(false))
			}

			if stop {
				break
			}
		}

		consumerDone <- true
	}()

	signals := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s := <-signals
		log.Printf("received signal: %v", s)

		closeCtx()

		<-consumerDone
		log.Printf("consumer done")
		log.Printf("close consumer channel, err: %v", consumeCh.Close())

		<-producerDone
		log.Printf("producer done")
		log.Printf("close producer channel, err: %v", sendCh.Close())

		log.Printf("close connection, err: %v", conn.Close())

		done <- true
	}()

	log.Printf("awaiting signal")
	<-done
	log.Printf("exiting")
}
