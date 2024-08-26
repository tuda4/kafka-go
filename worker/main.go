package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

var (
	topic = "orders"
)

func main() {

	// Connect consumer to Kafka
	brokers := []string{"localhost:9092"}
	worker, err := ConsumerConnect(brokers)
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	// Handle OS signals
	syncSignal := make(chan os.Signal, 1)
	signal.Notify(syncSignal, syscall.SIGINT, syscall.SIGTERM)
	// Create a goroutine to consume
	doneChannel := make(chan struct{})
	msgCount := 0
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println("Error: ", err)
			case msg := <-consumer.Messages():
				msgCount++
				order := string(msg.Value)
				fmt.Printf("Received count: %v| topic: %s | message: %s \n", msgCount, string(msg.Topic), string(msg.Value))
				fmt.Printf("Received order: %s \n", order)
			case <-syncSignal:
				doneChannel <- struct{}{}
				fmt.Println("Received a signal to stop consuming")
			}
		}
	}()
	// Close the consumer
	<-doneChannel
	fmt.Println("Closing consumer")
	if err := consumer.Close(); err != nil {
		panic(err)
	}
}

func ConsumerConnect(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	return sarama.NewConsumer(brokers, config)
}
