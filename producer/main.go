package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

var (
	topic = "orders"
)

type Order struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func main() {
	http.HandleFunc("/order", placeOrder)
	err := http.ListenAndServe(":8080", nil)
	log.Fatal(err)
}

func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll

	return sarama.NewSyncProducer(brokers, config)
}

func PushOrderToQueue(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}
	// create connection to producer
	producer, err := ConnectProducer(brokers)
	if err != nil {
		return err
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %v", err)
		}
	}()
	// send message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return err
	}

	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	return nil
}

func placeOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Parse the request
	order := new(Order)
	if err := json.NewDecoder(r.Body).Decode(order); err != nil {
		log.Printf("Error decoding order: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Create an order
	orderInBytes, err := json.Marshal(order)
	if err != nil {
		log.Printf("Error marshalling order: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Send the order to the producer Kafka topic
	if err = PushOrderToQueue(topic, orderInBytes); err != nil {
		log.Printf("Error pushing order to queue: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Return a success response
	res := map[string]any{
		"status":  "success",
		"message": "Order placed successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
