package mqtt

import (
	"context"
	"fmt"
	"time"

	"github.com/civet148/go-mqc/options"
	"github.com/civet148/go-mqc/types"
	"github.com/civet148/go-mqc/utils"
	"github.com/gonzalop/mq"
	"github.com/google/uuid"
)

type mqttClient struct {
	client *mq.Client
}

func NewClient(address string, opfs ...options.DialOption) (types.MQ, error) {
	// Connect to MQTT server
	var dialOptions options.DialOptions
	for _, opf := range opfs {
		opf(&dialOptions)
	}
	var mqttOptions = []mq.Option{
		mq.WithAutoReconnect(true),
		mq.WithAutoProtocolVersion(true),
		mq.WithClientID(dialOptions.ClientId),
		mq.WithKeepAlive(dialOptions.KeepAlive),
		mq.WithCleanSession(dialOptions.CleanSession),
		mq.WithMaxTopicLength(dialOptions.MaxTopicLength),
		mq.WithMaxPayloadSize(dialOptions.MaxPayloadSize),
		mq.WithMaxIncomingPacket(dialOptions.MaxIncomingPacket),
		mq.WithReconnectBackoff(dialOptions.ReconnectBackoff, dialOptions.MaxReconnectBackoff, true),
	}
	if dialOptions.ConnectTimeout > 0 {
		mqttOptions = append(mqttOptions, mq.WithConnectTimeout(dialOptions.ConnectTimeout))
	}
	if dialOptions.ProtocolVersion > 0 {
		mqttOptions = append(mqttOptions, mq.WithProtocolVersion(dialOptions.ProtocolVersion))
	}
	if !dialOptions.CleanSession && dialOptions.ClientId == "" {
		mqttOptions = append(mqttOptions, mq.WithClientID(uuid.New().String()))
	} else {
		mqttOptions = append(mqttOptions, mq.WithClientID(dialOptions.ClientId))
	}
	client, err := mq.Dial(address, mqttOptions...)
	if err != nil {
		return nil, fmt.Errorf("dial mqtt server %s error: %s", address, err.Error())
	}
	return &mqttClient{
		client: client,
	}, nil
}

func (c *mqttClient) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

func (c *mqttClient) Publish(ctx context.Context, topic string, msg any, opfs ...options.PublishOption) error {
	var timeout = 2 * time.Second
	var opts options.PublishOptions
	for _, opf := range opfs {
		opf(&opts)
	}
	var publishOptions []mq.PublishOption
	if opts.Qos != 0 {
		publishOptions = append(publishOptions, mq.WithQoS(mq.QoS(opts.Qos)))
	}
	if opts.Retain {
		publishOptions = append(publishOptions, mq.WithRetain(true))
	}
	var cancel context.CancelFunc
	if opts.Timeout > 0 {
		timeout = opts.Timeout
	}
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()
	for k, v := range opts.UserProperties {
		publishOptions = append(publishOptions, mq.WithUserProperty(k, v))
	}
	data := utils.MarshalPublishMsg(msg)
	token := c.client.Publish(ctx, topic, data, publishOptions...)
	if err := token.Wait(ctx); err != nil {
		return err
	}
	return nil
}

func (c *mqttClient) Subscribe(ctx context.Context, topic string, handler types.MessageHandler, opfs ...options.SubscribeOption) error {
	var opts options.SubscribeOptions
	for _, opf := range opfs {
		opf(&opts)
	}
	var subscribeOptions []mq.SubscribeOption
	if opts.SubscriptionID > 0 {
		subscribeOptions = append(subscribeOptions, mq.WithSubscriptionIdentifier(opts.SubscriptionID))
	}
	for k, v := range opts.UserProperties {
		subscribeOptions = append(subscribeOptions, mq.WithSubscribeUserProperty(k, v))
	}
	token := c.client.Subscribe(ctx, topic, mq.QoS(opts.Qos), func(client *mq.Client, message mq.Message) {
		_ = handler(message.Topic, message.Payload)
	}, subscribeOptions...)
	if err := token.Wait(ctx); err != nil {
		return err
	}
	if opts.Block {
		utils.BlockRoutine()
	}
	return nil
}
