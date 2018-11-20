package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	ch := make(chan string)

	go produceTextMessage(producer)
	go consumeTextMessage(consumer, ch)
	go handle(ch)

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM)

	<-sig
}

func produceTextMessage(producer sarama.AsyncProducer) {
	for i := 0; ; i++ {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "wingyplus",
			Key:   nil,
			Value: sarama.StringEncoder(fmt.Sprintf("From producer %d", i)),
		}
		time.Sleep(1 * time.Second)
	}
}

// consumeTextMessage consume text message from kafka producer and send text message to ch.
func consumeTextMessage(producer sarama.Consumer, ch chan<- string) {
	pc, err := producer.ConsumePartition("wingyplus", 0, sarama.OffsetNewest)
	if err != nil {
		// NOTE: this is bad practice. don't do this in production!!!
		panic(err)
	}

	for msg := range pc.Messages() {
		ch <- string(msg.Value)
	}
}

// handle receive text message from ch and print to standard output.
func handle(ch <-chan string) {
	for s := range ch {
		fmt.Println(s)
	}
}
