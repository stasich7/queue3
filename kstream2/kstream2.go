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
	brokers               = []string{"localhost:9092"} //, "localhost:9093", "localhost:9094"}
	topic   []goka.Stream = []goka.Stream{"a3", "a4", "a5"}
	group   goka.Group    = "kstream2-group"
	output  goka.Stream   = "output3"
	sigs                  = make(chan os.Signal)

	tmc *goka.TopicManagerConfig
)

func randomStorageBuilder(suffix string) storage.Builder {
	return storage.DefaultBuilder(fmt.Sprintf("/tmp/goka-%d/%s", time.Now().Unix(), suffix))
}

func runProcessor() {
	cb := func(ctx goka.Context, msg interface{}) {
		val := ctx.Value()
		// val, ok := msg.(string)
		// if !ok {
		// 	log.Printf("type assertion error, %#v\n", ctx.Value())
		// }
		ctx.Emit(output, "key", val)
		log.Printf("forward from [%s] val = [%s]", ctx.Topic(), val)
		ctx.SetValue(val)
	}

	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1

	p, err := goka.NewProcessor(brokers,
		goka.DefineGroup(group,
			goka.Input(goka.Stream(topic[0]), new(codec.String), cb),
			goka.Input(goka.Stream(topic[1]), new(codec.String), cb),
			goka.Input(goka.Stream(topic[2]), new(codec.String), cb),
			goka.Output(output, new(codec.String)),
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

	select {
	case <-sigs:
	case <-done:
	}
	cancel()
	<-done
}

func runView() {
	view, err := goka.NewView(brokers,
		goka.GroupTable(group),
		new(codec.String),
	)
	if err != nil {
		fmt.Println(err)
	}
	go func() {
		t := time.NewTicker(time.Second * 3)
		for range t.C {
			val, _ := view.Get("key")
			fmt.Printf("view: %#v\n", val)

			select {
			case <-sigs:
				return
			default:
			}

		}
	}()
	err = view.Run(context.Background())
	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	config := goka.DefaultConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	goka.ReplaceGlobalConfig(config)

	go func() {
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	}()

	// tm, err := goka.NewTopicManager(brokers, goka.DefaultConfig(), tmc)
	// if err != nil {
	// 	log.Fatalf("Error creating topic manager: %v", err)
	// }
	// defer tm.Close()
	// err = tm.EnsureStreamExists(string(topic), 8)
	// if err != nil {
	// 	log.Printf("Error creating kafka topic %s: %v", topic, err)
	// }
	go runProcessor()
	go runView()

	<-sigs
}
