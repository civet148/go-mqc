package mqtt

import (
	"context"
	"fmt"

	"github.com/civet148/go-mqc/options"
	"github.com/civet148/go-mqc/types"
	"github.com/civet148/go-mqc/utils"
	"github.com/gonzalop/mq"
	"github.com/google/uuid"
)

type mqttClient struct {
	client  *mq.Client
	handler types.MessageHandler
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
	data := utils.MarshalPublishMsg(msg)
	token := c.client.Publish(ctx, topic, data)
	if err := token.Error(); err != nil {
		return err
	}
	return nil
}

func (c *mqttClient) Subscribe(ctx context.Context, topic string, handler types.MessageHandler, opfs ...options.SubscribeOption) error {
	var subOptions options.SubscribeOptions
	for _, opf := range opfs {
		opf(&subOptions)
	}
	c.handler = handler
	token := c.client.Subscribe(ctx, topic, mq.QoS(subOptions.Qos), c.mqttMsgHandler)
	if err := token.Error(); err != nil {
		return err
	}
	if subOptions.Block {
		utils.BlockRoutine()
	}
	return nil
}

func (c *mqttClient) mqttMsgHandler(client *mq.Client, msg mq.Message) {
	c.handler(msg.Topic, msg.Payload)
}
