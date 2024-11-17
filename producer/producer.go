package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

const (
	tTopic     = "singlefile_%s"
	brokers    = "localhost:9092,localhost:9093"
	intervalMs = 1000
)

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

	clusterAdmin, _ := sarama.NewClusterAdmin(strings.Split(brokers, ","), sarama.NewConfig())

	err := sarama.ClusterAdmin.CreateTopic(clusterAdmin, topic, &sarama.TopicDetail{
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

	i := 0
	t := time.NewTicker(time.Millisecond * intervalMs)
	for range t.C {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(strconv.Itoa(i)),
		}
		i++

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Message [%d] sent partition(%d)/offset(%d)\n", i, partition, offset)
	}

}
