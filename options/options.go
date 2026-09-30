package options

import (
	"crypto/tls"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type ExchangeKind string

const (
	ExchangeKindDirect  ExchangeKind = amqp091.ExchangeDirect
	ExchangeKindTopic   ExchangeKind = amqp091.ExchangeTopic
	ExchangeKindFanout  ExchangeKind = amqp091.ExchangeFanout
	ExchangeKindHeaders ExchangeKind = amqp091.ExchangeHeaders
)

type DialOptions struct {
	User                string        // 用户名
	Passwd              string        // 密码
	ClientId            string        // 客户端ID
	KeepAlive           time.Duration // 保活时间
	ConnectTimeout      time.Duration // 连接超时时间
	CleanSession        bool          // MQTT: 清理会话(为true时必须指定客户端ID)
	TLSConfig           *tls.Config   // TLS配置
	ReconnectBackoff    time.Duration // 重连退避时间(默认：1秒)
	MaxReconnectBackoff time.Duration // 最大重连退避时间(默认：2分钟)
	IncomingQueueSize   int           // 入站队列大小
	ProtocolVersion     uint8         // MQTT协议版本 (4 = v3.1.1, 5 = v5.0)
	MaxTopicLength      int           // topic最大长度 (MQTT default: 1024)
	MaxPayloadSize      int           // 最大负载数据大小 (MQTT default: 1MB)
	MaxIncomingPacket   int           // 最大入站数据包大小 (MQTT default: 1MB)
	ExchangeName        string        // 交换机名称(仅限RabbitMQ)
	ExchangeKind        ExchangeKind  // 交换机类型(仅限RabbitMQ)
}

type DialOption func(opts *DialOptions)

func WithExchangeKind(exchangeKind ExchangeKind) DialOption {
	return func(opts *DialOptions) {
		opts.ExchangeKind = exchangeKind
	}
}

func WithExchangeName(exchangeName string) DialOption {
	return func(opts *DialOptions) {
		opts.ExchangeName = exchangeName
	}
}

func WithUser(user string) DialOption {
	return func(opts *DialOptions) {
		opts.User = user
	}
}

func WithPasswd(passwd string) DialOption {
	return func(opts *DialOptions) {
		opts.Passwd = passwd
	}
}

func WithClientID(clientId string) DialOption {
	return func(opts *DialOptions) {
		opts.ClientId = clientId
	}
}

func WithKeepAlive(keepAlive time.Duration) DialOption {
	return func(opts *DialOptions) {
		opts.KeepAlive = keepAlive
	}
}

func WithConnectTimeout(connectTimeout time.Duration) DialOption {
	return func(opts *DialOptions) {
		opts.ConnectTimeout = connectTimeout
	}
}

func WithCleanSession(cleanSession bool) DialOption {
	return func(opts *DialOptions) {
		opts.CleanSession = cleanSession
	}
}
func WithTLSConfig(tlsConfig *tls.Config) DialOption {
	return func(opts *DialOptions) {
		opts.TLSConfig = tlsConfig
	}
}

func WithReconnectBackoff(reconnectBackoff time.Duration) DialOption {
	return func(opts *DialOptions) {
		opts.ReconnectBackoff = reconnectBackoff
	}
}

func WithMaxReconnectBackoff(maxReconnectBackoff time.Duration) DialOption {
	return func(opts *DialOptions) {
		opts.MaxReconnectBackoff = maxReconnectBackoff
	}
}
func WithIncomingQueueSize(incomingQueueSize int) DialOption {
	return func(opts *DialOptions) {
		opts.IncomingQueueSize = incomingQueueSize
	}
}
func WithProtocolVersion(protocolVersion uint8) DialOption {
	return func(opts *DialOptions) {
		opts.ProtocolVersion = protocolVersion
	}
}
func WithMaxTopicLength(maxTopicLength int) DialOption {
	return func(opts *DialOptions) {
		opts.MaxTopicLength = maxTopicLength
	}
}
func WithMaxPayloadSize(maxPayloadSize int) DialOption {
	return func(opts *DialOptions) {
		opts.MaxPayloadSize = maxPayloadSize
	}
}
func WithMaxIncomingPacket(maxIncomingPacket int) DialOption {
	return func(opts *DialOptions) {
		opts.MaxIncomingPacket = maxIncomingPacket
	}
}

/*-----------------------------------------------------------------------------------------------------------*/

type PublishOptions struct {
	Qos         uint8    // QoS 0: 最多一次 1: 最少一次 2: 只一次
	Retain      bool     // 是否保留消息
	RoutingKeys []string // 一次发布到多个路由键

}

type PublishOption func(opts *PublishOptions)

func WithPubQos(qos uint8) PublishOption {
	return func(opts *PublishOptions) {
		opts.Qos = qos
	}
}

func WithRetain(retain bool) PublishOption {
	return func(opts *PublishOptions) {
		opts.Retain = retain
	}
}

func WithRoutingKeys(routingKeys ...string) PublishOption {
	return func(opts *PublishOptions) {
		opts.RoutingKeys = routingKeys
	}
}

/*-----------------------------------------------------------------------------------------------------------*/

type SubscribeOptions struct {
	Qos         uint8  // QoS 0: 最多一次 1: 最少一次 2: 只一次 (仅MQTT)
	NackDiscard bool   // 丢弃消息或进入死信队列(仅限RabbitMQ)
	QueueName   string // 队列名称(仅限RabbitMQ)
	Block       bool   // 阻塞模式
}

type SubscribeOption func(opts *SubscribeOptions)

func WithBlock(block bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Block = block
	}
}

func WithQueueName(queueName string) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.QueueName = queueName
	}
}

func WithSubQos(qos uint8) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Qos = qos
	}
}

func WithNackDiscard() SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.NackDiscard = true
	}
}

/*-----------------------------------------------------------------------------------------------------------*/

type MessageOptions struct {
	DeliveryMode  uint8     // queue implementation use - non-persistent (1) or persistent (2)
	Priority      uint8     // queue implementation use - 0 to 9
	CorrelationId string    // application use - correlation identifier
	ReplyTo       string    // application use - address to reply to (ex: RPC)
	Expiration    string    // implementation use - message expiration spec
	MessageId     string    // application use - message identifier
	Timestamp     time.Time // application use - message timestamp
	Type          string    // application use - message type name
	UserId        string    // application use - creating user - should be authenticated user
	AppId         string    // application use - creating application id
}
type MessageOption func(opts *MessageOptions)

func WithDeliveryMode(deliveryMode uint8) MessageOption {
	return func(opts *MessageOptions) {
		opts.DeliveryMode = deliveryMode
	}
}
func WithPriority(priority uint8) MessageOption {
	return func(opts *MessageOptions) {
		opts.Priority = priority
	}
}
func WithMessageId(messageId string) MessageOption {
	return func(opts *MessageOptions) {
		opts.MessageId = messageId
	}
}

func WithTimestamp(timestamp time.Time) MessageOption {
	return func(opts *MessageOptions) {
		opts.Timestamp = timestamp
	}
}

func WithType(typ string) MessageOption {
	return func(opts *MessageOptions) {
		opts.Type = typ
	}
}

func WithUserId(userId string) MessageOption {
	return func(opts *MessageOptions) {
		opts.UserId = userId
	}
}
func WithAppId(appId string) MessageOption {
	return func(opts *MessageOptions) {
		opts.AppId = appId
	}
}

func WithReplyTo(replyTo string) MessageOption {
	return func(opts *MessageOptions) {
		opts.ReplyTo = replyTo
	}
}
func WithExpiration(expiration string) MessageOption {
	return func(opts *MessageOptions) {
		opts.Expiration = expiration
	}
}

func WithCorrelationId(correlationId string) MessageOption {
	return func(opts *MessageOptions) {
		opts.CorrelationId = correlationId
	}
}
