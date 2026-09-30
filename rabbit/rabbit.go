package rabbit

import (
	"context"
	"fmt"

	"github.com/civet148/go-mqc/options"
	"github.com/civet148/go-mqc/types"
	"github.com/civet148/go-mqc/utils"
	"github.com/civet148/log"
	"github.com/wagslane/go-rabbitmq"
)

type rabbitClient struct {
	client      *rabbitmq.Conn
	dialOptions options.DialOptions
	publisher   *rabbitmq.Publisher
	consumers   []*rabbitmq.Consumer
}

func NewClient(address string, opfs ...options.DialOption) (types.MQ, error) {
	var dialOptions options.DialOptions
	for _, opf := range opfs {
		opf(&dialOptions)
	}
	if dialOptions.ExchangeName == "" {
		return nil, fmt.Errorf("exchange name is empty")
	}
	if dialOptions.ExchangeKind == "" {
		dialOptions.ExchangeKind = options.ExchangeKindTopic
	}
	var config rabbitmq.Config
	if dialOptions.TLSConfig != nil {
		config.TLSClientConfig = dialOptions.TLSConfig
	}
	conn, err := rabbitmq.NewConn(
		address,
		rabbitmq.WithConnectionOptionsConfig(config),
	)
	if err != nil {
		return nil, err
	}
	return &rabbitClient{
		client:      conn,
		dialOptions: dialOptions,
	}, nil
}

func (c *rabbitClient) Publish(ctx context.Context, topic string, msg any, opfs ...options.PublishOption) error {
	var pubOptions options.PublishOptions
	for _, opf := range opfs {
		opf(&pubOptions)
	}
	var publisherOptions = []func(*rabbitmq.PublisherOptions){
		rabbitmq.WithPublisherOptionsExchangeName(c.exchangeName()),
		rabbitmq.WithPublisherOptionsExchangeKind(c.exchangeKind()),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
	}
	if c.publisher == nil {
		publisher, err := rabbitmq.NewPublisher(
			c.client,
			publisherOptions...,
		)
		if err != nil {
			return log.Errorf("new publisher error: %s", err)
		}
		c.publisher = publisher
	}
	var routingKeys = []string{topic}
	var publishOptions = []func(*rabbitmq.PublishOptions){
		rabbitmq.WithPublishOptionsExchange(c.exchangeName()),
	}
	if len(pubOptions.RoutingKeys) != 0 {
		routingKeys = append(routingKeys, pubOptions.RoutingKeys...)
	}
	publishOptions = append(publishOptions, c.parsePublishOptions(pubOptions)...)

	data := utils.MarshalPublishMsg(msg)
	err := c.publisher.Publish(data, routingKeys, publishOptions...)
	if err != nil {
		return log.Errorf("exchange [%s] routing key %v publish message error: %s", c.exchangeName(), routingKeys, err)
	}
	return nil
}

func (c *rabbitClient) parsePublishOptions(pubOptions options.PublishOptions) (publishOptions []func(*rabbitmq.PublishOptions)) {
	if pubOptions.ContentEncoding != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsContentEncoding(pubOptions.ContentEncoding))
	}
	if pubOptions.ContentType != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsContentType(pubOptions.ContentType))
	}
	if pubOptions.CorrelationID != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsCorrelationID(pubOptions.CorrelationID))
	}
	if pubOptions.ReplyTo != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsReplyTo(pubOptions.ReplyTo))
	}
	if pubOptions.Priority != 0 {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsPriority(pubOptions.Priority))
	}
	if pubOptions.MessageID != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsMessageID(pubOptions.MessageID))
	}
	if pubOptions.Type != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsType(pubOptions.Type))
	}
	if pubOptions.UserID != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsUserID(pubOptions.UserID))
	}
	if pubOptions.AppID != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsAppID(pubOptions.AppID))
	}
	if pubOptions.Timestamp.Unix() != 0 {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsTimestamp(pubOptions.Timestamp))
	}
	if pubOptions.Expiration != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsExpiration(pubOptions.Expiration))
	}
	if pubOptions.DeliveryMode == types.DeliveryModePersistent {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsPersistentDelivery)
	}
	return publishOptions
}

func (c *rabbitClient) Subscribe(ctx context.Context, topic string, handler types.MessageHandler, opfs ...options.SubscribeOption) error {
	var subOptions options.SubscribeOptions
	for _, opf := range opfs {
		opf(&subOptions)
	}
	if subOptions.QueueName == "" {
		return fmt.Errorf("queue name is empty")
	}
	consumer, err := rabbitmq.NewConsumer(
		c.client,
		subOptions.QueueName,
		rabbitmq.WithConsumerOptionsRoutingKey(topic),
		rabbitmq.WithConsumerOptionsExchangeName(c.exchangeName()),
		rabbitmq.WithConsumerOptionsExchangeKind(c.exchangeKind()),
		rabbitmq.WithConsumerOptionsExchangeDeclare,
	)
	if err != nil {
		return err
	}
	c.consumers = append(c.consumers, consumer)
	go func() {
		err = consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
			var msgOptions = []options.MessageOption{
				options.WithMsgExpiration(d.Expiration),
				options.WithMsgCorrelationID(d.CorrelationId),
				options.WithMsgDeliveryMode(d.DeliveryMode),
				options.WithMsgPriority(d.Priority),
				options.WithMsgReplyTo(d.ReplyTo),
				options.WithMsgTimestamp(d.Timestamp),
				options.WithMsgCorrelationID(d.CorrelationId),
				options.WithMsgMessageId(d.MessageId),
				options.WithMsgType(d.Type),
				options.WithMsgUserID(d.UserId),
				options.WithMsgAppID(d.AppId),
			}
			if err = handler(d.RoutingKey, d.Body, msgOptions...); err != nil {
				if subOptions.NackDiscard {
					return rabbitmq.NackDiscard
				}
				return rabbitmq.NackRequeue
			}
			return rabbitmq.Ack
		})
		if err != nil {
			log.Errorf("consume exchange [%s] topic [%s] error: %s", c.exchangeName(), topic, err.Error())
		}
	}()

	if subOptions.Block {
		utils.BlockRoutine()
	}
	return nil
}

func (c *rabbitClient) Close(ctx context.Context) error {
	for _, consumer := range c.consumers {
		consumer.Close()
	}
	return c.client.Close()
}

func (c *rabbitClient) exchangeName() string {
	return c.dialOptions.ExchangeName
}

func (c *rabbitClient) exchangeKind() string {
	return string(c.dialOptions.ExchangeKind)
}
