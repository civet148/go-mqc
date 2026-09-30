package main

import (
	"context"
	"fmt"
	"time"

	"github.com/civet148/go-mqc"
	"github.com/civet148/go-mqc/options"
	"github.com/civet148/go-mqc/types"
	"github.com/civet148/log"
)

const (
	address        = "tcp://admin:public@127.0.0.1:1883"
	publishTopic   = "/mqc/pub1"
	subscribeTopic = "/mqc/+"
)

func main() {
	var ctx = context.Background()
	client, err := mqc.NewMQ(address)
	if err != nil {
		panic(err.Error())
	}

	// 异步启动消费者
	go runConsumer(ctx, client)

	if err = runPublisher(ctx, client); err != nil {
		log.Panic(err.Error())
	}
	time.Sleep(1 * time.Minute)
	log.Infof("Press Ctrl-C to exit...")
	var ch = make(chan bool)
	<-ch
}
func runPublisher(ctx context.Context, client types.MQ) (err error) {
	// 发布10条测试消息
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		var msg = fmt.Sprintf("hello %v", i+1)
		if err = client.Publish(ctx, publishTopic, msg); err != nil {
			panic(err)
		}
		log.Infof("Publish routing key [%s] message [%v]", publishTopic, msg)
	}
	return nil
}

func runConsumer(ctx context.Context, client types.MQ) (err error) {
	log.Infof("start subscribe topic [%s]", subscribeTopic)
	err = client.Subscribe(ctx, subscribeTopic, messageHandle)
	if err != nil {
		return err
	}
	return nil
}

func messageHandle(topic string, data any, opfs ...options.MessageOption) error {
	var msgOptions options.MessageOptions
	for _, opf := range opfs {
		opf(&msgOptions)
	}
	log.Infof("Received message on topic [%s] data [%s]", topic, data)
	time.Sleep(100 * time.Millisecond)
	return nil
}
