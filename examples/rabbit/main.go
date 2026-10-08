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
	address        = "amqp://admin:12345678@127.0.0.1:5672"
	publishTopic   = "mqc.pub1"
	subscribeTopic = "mqc.#"
)

func main() {
	var ctx = context.Background()

	client, err := mqc.NewMQ(address, options.WithDialExchangeName("order"), options.WithDialDeliveryMode(types.DeliveryModePersistent))
	if err != nil {
		log.Panic(err.Error())
	}
	defer client.Close(ctx)

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
	// 发布测试消息
	for i := 0; i < 10000; i++ {
		var msg = fmt.Sprintf("hello %v", i+1)
		if err = client.Publish(ctx, publishTopic, []byte(msg),
			options.WithPubPriority(3),
			options.WithPubAppID("2026001"),
			options.WithPubMessageID(fmt.Sprintf("%v", i)),
			options.WithPubContentEncoding(types.ContentEncoding_UTF8),
			options.WithPubContentType(types.ContentType_ApplicationJSON),
		); err != nil {
			log.Errorf("publish error: %s", err.Error())
		} else {
			log.Infof("Publish routing key [%s] message [%v]", publishTopic, msg)
		}
		time.Sleep(3 * time.Second)
	}
	return nil
}

func runConsumer(ctx context.Context, client types.MQ) (err error) {
	err = client.Subscribe(ctx, subscribeTopic, messageHandle1, options.WithSubQueueName("order_queue_1"), options.WithSubCustomerTag("order-customer-1"))
	if err != nil {
		return log.Errorf("Subscribe topic [%s] error: %s", subscribeTopic, err)
	}
	err = client.Subscribe(ctx, subscribeTopic, messageHandle2, options.WithSubQueueName("order_queue_1"), options.WithSubCustomerTag("order-customer-2"))
	if err != nil {
		return log.Errorf("Subscribe topic [%s] error: %s", subscribeTopic, err)
	}
	return nil
}

func messageHandle1(topic string, data []byte, opfs ...options.MessageOption) error {
	var msgOptions options.MessageOptions
	for _, opf := range opfs {
		opf(&msgOptions)
	}
	log.Infof("[consumer1] Received message on topic [%s] data [%s] msg options [%+v]", topic, data, msgOptions)
	time.Sleep(100 * time.Millisecond)
	return nil
}
func messageHandle2(topic string, data []byte, opfs ...options.MessageOption) error {
	var msgOptions options.MessageOptions
	for _, opf := range opfs {
		opf(&msgOptions)
	}
	log.Infof("[consumer2] Received message on topic [%s] data [%s] msg options [%+v]", topic, data, msgOptions)
	time.Sleep(100 * time.Millisecond)
	return nil
}
