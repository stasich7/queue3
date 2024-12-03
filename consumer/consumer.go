package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

const (
	assignor = ""
	brokers  = "localhost:9092,localhost:9093,localhost:9094"
	tTopic   = "%s"
)

func main() {
	var (
		new   bool
		id    string
		group uuid.UUID
	)
	flag.StringVar(&id, "id", "", "")
	flag.BoolVar(&new, "new", false, "")
	flag.Parse()
	topic := fmt.Sprintf(tTopic, id)

	config := sarama.NewConfig()

	if new {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	consumer := Consumer{
		ready: make(chan bool),
	}

	group, _ = uuid.NewRandom()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group.String(), config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, strings.Split(topic, ","), &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready
	log.Println("Consumer started for topic", topic, " group ID: ", group)

	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closingt: %v", err)
	}
}

type Consumer struct {
	ready chan bool
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// var prev int
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}

			val := string(message.Value)
			key := message.Key
			log.Printf("Message: p %d o = %d key = %s val = %s", message.Partition, message.Offset, key, val)
			// if prev != 0 && val != prev+1 {
			// 	log.Printf("Warning")
			// }
			// prev = val
		case <-session.Context().Done():
			return nil
		}
	}
}
