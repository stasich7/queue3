package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

const (
	tTopic     = "%s"
	brokers    = "localhost:9092" //,localhost:9093,localhost:9094"
	intervalMs = 1000
)

var (
	fruit = [...]string{"🍒", "🍋", "🌵"}
)

type Msg struct {
	Key   string
	Value string
}

func main() {
	var (
		id string
		p  int
		r  int
	)

	flag.StringVar(&id, "id", "", "")
	flag.IntVar(&p, "p", 1, "")
	flag.IntVar(&r, "r", 1, "")
	flag.Parse()
	topic := fmt.Sprintf(tTopic, id)

	clusterAdmin, err := sarama.NewClusterAdmin(strings.Split(brokers, ","), sarama.NewConfig())
	if err != nil {
		log.Panic(err)
	}

	err = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(p),
		ReplicationFactor: int16(r),
	}, false)
	if err != nil {
		log.Println(err)
	}
	log.Printf("Created topic %s partitions %d repicas %d", topic, p, r)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
		// clusterAdmin.DeleteTopic(topic)
	}()

	t := time.NewTicker(time.Millisecond * intervalMs)
	for range t.C {
		fruit := fruit[rand.Intn(len(fruit))]
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("slot_%s", topic)),
			Value: sarama.StringEncoder(fruit),
		}

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Message [%s] sent partition(%d)/offset(%d)\n", fruit, partition, offset)
	}

}
