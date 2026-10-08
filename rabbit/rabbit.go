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
	var opts options.DialOptions
	for _, opf := range opfs {
		opf(&opts)
	}
	if opts.ExchangeName == "" {
		return nil, fmt.Errorf("exchange name is empty")
	}
	if opts.ExchangeKind == "" {
		opts.ExchangeKind = options.ExchangeKindTopic
	}
	var config rabbitmq.Config
	if opts.TLSConfig != nil {
		config.TLSClientConfig = opts.TLSConfig
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
		dialOptions: opts,
	}, nil
}

func (c *rabbitClient) Publish(ctx context.Context, topic string, msg any, opfs ...options.PublishOption) error {
	var opts options.PublishOptions
	for _, opf := range opfs {
		opf(&opts)
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
	if len(opts.RoutingKeys) != 0 {
		routingKeys = append(routingKeys, opts.RoutingKeys...)
	}
	publishOptions = append(publishOptions, c.parsePublishOptions(opts)...)

	data := utils.MarshalPublishMsg(msg)
	err := c.publisher.Publish(data, routingKeys, publishOptions...)
	if err != nil {
		return log.Errorf("exchange [%s] routing key [%v] publish message error: %s", c.exchangeName(), routingKeys, err)
	}
	return nil
}

func (c *rabbitClient) parsePublishOptions(opts options.PublishOptions) (publishOptions []func(*rabbitmq.PublishOptions)) {
	if opts.ContentEncoding != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsContentEncoding(opts.ContentEncoding))
	}
	if opts.ContentType != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsContentType(opts.ContentType))
	}
	if opts.CorrelationID != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsCorrelationID(opts.CorrelationID))
	}
	if opts.ReplyTo != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsReplyTo(opts.ReplyTo))
	}
	if opts.Priority != 0 {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsPriority(opts.Priority))
	}
	if opts.MessageID != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsMessageID(opts.MessageID))
	}
	if opts.Type != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsType(opts.Type))
	}
	if opts.UserID != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsUserID(opts.UserID))
	}
	if opts.AppID != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsAppID(opts.AppID))
	}
	if opts.Timestamp.Unix() != 0 {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsTimestamp(opts.Timestamp))
	}
	if opts.Expiration != "" {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsExpiration(opts.Expiration))
	}
	if opts.DeliveryMode == 0 {
		if c.dialOptions.DeliveryMode == types.DeliveryModePersistent {
			publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsPersistentDelivery)
		}
	} else if opts.DeliveryMode == types.DeliveryModePersistent {
		publishOptions = append(publishOptions, rabbitmq.WithPublishOptionsPersistentDelivery)
	}
	return publishOptions
}

func (c *rabbitClient) Subscribe(ctx context.Context, topic string, handler types.MessageHandler, opfs ...options.SubscribeOption) error {
	var opts options.SubscribeOptions
	for _, opf := range opfs {
		opf(&opts)
	}
	if opts.QueueName == "" {
		return fmt.Errorf("queue name is empty")
	}
	consumer, err := rabbitmq.NewConsumer(
		c.client,
		opts.QueueName,
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
				if opts.NackDiscard {
					return rabbitmq.NackDiscard
				}
				return rabbitmq.NackRequeue
			}
			return rabbitmq.Ack
		})
		if err != nil {
			log.Errorf("exchange [%s] consume topic [%s] error: %s", c.exchangeName(), topic, err.Error())
		}
	}()

	if opts.Block {
		utils.BlockRoutine()
	}
	return nil
}

func (c *rabbitClient) Close(ctx context.Context) error {
	for _, consumer := range c.consumers {
		consumer.Close()
	}
	c.publisher.Close()
	return c.client.Close()
}

func (c *rabbitClient) exchangeName() string {
	return c.dialOptions.ExchangeName
}

func (c *rabbitClient) exchangeKind() string {
	return string(c.dialOptions.ExchangeKind)
}
