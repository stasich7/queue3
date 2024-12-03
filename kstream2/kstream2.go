package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
)

var (
	brokers            = []string{"localhost:9092", "localhost:9093", "localhost:9094"}
	topic   []string   = []string{"singlefile_25", "singlefile_26", "singlefile_26"}
	group   goka.Group = "kstream2-group"

	tmc *goka.TopicManagerConfig
)

func randomStorageBuilder(suffix string) storage.Builder {
	return storage.DefaultBuilder(fmt.Sprintf("/tmp/goka-%d/%s", time.Now().Unix(), suffix))
}

func runProcessor() {
	cb := func(ctx goka.Context, msg interface{}) {
		ctx.Emit("output", ctx.Key(), fmt.Sprintf("forwarded: %v", msg))
		log.Printf("offset = %v", ctx.Offset())
	}

	tmc = goka.NewTopicManagerConfig()
	// tmc.Table.Replication = 1
	// tmc.Stream.Replication = 1

	p, err := goka.NewProcessor(brokers,
		goka.DefineGroup(group,
			goka.Input(goka.Stream(topic[0]), new(codec.String), cb),
			goka.Input(goka.Stream(topic[1]), new(codec.String), cb),
			goka.Output("output", new(codec.String)),
			goka.Persist(new(codec.String)),
		),
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
		goka.WithStorageBuilder(randomStorageBuilder("proc")),
	)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Printf("error running processor: %v", err)
		}
	}()

	sigs := make(chan os.Signal)
	go func() {
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	}()

	select {
	case <-sigs:
	case <-done:
	}
	cancel()
	<-done
}

func main() {
	config := goka.DefaultConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(config)

	// tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	// if err != nil {
	// 	log.Fatalf("Error creating topic manager: %v", err)
	// }
	// defer tm.Close()
	// err = tm.EnsureStreamExists(string(topic), 8)
	// if err != nil {
	// 	log.Printf("Error creating kafka topic %s: %v", topic, err)
	// }

	runProcessor()
}
