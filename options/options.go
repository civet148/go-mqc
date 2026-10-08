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
	ExchangeKind        ExchangeKind  // 交换机类型(仅限RabbitMQ,默认：topic模式)
	DeliveryMode        uint8         // 持久化控制：1=非持久化，2=持久化
}

type DialOption func(opts *DialOptions)

func WithDialExchangeKind(exchangeKind ExchangeKind) DialOption {
	return func(opts *DialOptions) {
		opts.ExchangeKind = exchangeKind
	}
}

func WithDialExchangeName(exchangeName string) DialOption {
	return func(opts *DialOptions) {
		opts.ExchangeName = exchangeName
	}
}
func WithDialDeliveryMode(deliveryMode uint8) DialOption {
	return func(opts *DialOptions) {
		opts.DeliveryMode = deliveryMode
	}
}
func WithDialUser(user string) DialOption {
	return func(opts *DialOptions) {
		opts.User = user
	}
}

func WithDialPasswd(passwd string) DialOption {
	return func(opts *DialOptions) {
		opts.Passwd = passwd
	}
}

func WithDialClientID(clientId string) DialOption {
	return func(opts *DialOptions) {
		opts.ClientId = clientId
	}
}

func WithDialKeepAlive(keepAlive time.Duration) DialOption {
	return func(opts *DialOptions) {
		opts.KeepAlive = keepAlive
	}
}

func WithDialConnectTimeout(connectTimeout time.Duration) DialOption {
	return func(opts *DialOptions) {
		opts.ConnectTimeout = connectTimeout
	}
}

func WithDialCleanSession(cleanSession bool) DialOption {
	return func(opts *DialOptions) {
		opts.CleanSession = cleanSession
	}
}
func WithDialTLSConfig(tlsConfig *tls.Config) DialOption {
	return func(opts *DialOptions) {
		opts.TLSConfig = tlsConfig
	}
}

func WithDialReconnectBackoff(reconnectBackoff time.Duration) DialOption {
	return func(opts *DialOptions) {
		opts.ReconnectBackoff = reconnectBackoff
	}
}

func WithDialMaxReconnectBackoff(maxReconnectBackoff time.Duration) DialOption {
	return func(opts *DialOptions) {
		opts.MaxReconnectBackoff = maxReconnectBackoff
	}
}
func WithDialIncomingQueueSize(incomingQueueSize int) DialOption {
	return func(opts *DialOptions) {
		opts.IncomingQueueSize = incomingQueueSize
	}
}
func WithDialProtocolVersion(protocolVersion uint8) DialOption {
	return func(opts *DialOptions) {
		opts.ProtocolVersion = protocolVersion
	}
}
func WithDialMaxTopicLength(maxTopicLength int) DialOption {
	return func(opts *DialOptions) {
		opts.MaxTopicLength = maxTopicLength
	}
}
func WithDialMaxPayloadSize(maxPayloadSize int) DialOption {
	return func(opts *DialOptions) {
		opts.MaxPayloadSize = maxPayloadSize
	}
}
func WithDialMaxIncomingPacket(maxIncomingPacket int) DialOption {
	return func(opts *DialOptions) {
		opts.MaxIncomingPacket = maxIncomingPacket
	}
}

/*-----------------------------------------------------------------------------------------------------------*/

type PublishOptions struct {
	Qos             uint8     // MQTT: QoS 0: 最多一次 1: 最少一次 2: 只一次
	Retain          bool      // 是否保留消息
	RoutingKeys     []string  // 一次发布到多个路由键
	ContentType     string    // MIME类型，如: "application/json"
	DeliveryMode    uint8     //	持久化控制：1=非持久化，2=持久化
	Expiration      string    //	消息级TTL，单位毫秒，字符串格式(例如：24小时="86400000")
	ContentEncoding string    //	字符编码，如: "utf-8"
	Priority        uint8     //	优先级(0-9)
	CorrelationID   string    //	RPC关联ID
	ReplyTo         string    //	RPC回复队列
	MessageID       string    //	消息唯一标识
	Timestamp       time.Time //	消息时间戳
	Type            string    //	应用自定义类型名
	UserID          string    //	消息创建用户ID
	AppID           string    //	消息创建应用ID
}

type PublishOption func(opts *PublishOptions)

func WithPubQos(qos uint8) PublishOption {
	return func(opts *PublishOptions) {
		opts.Qos = qos
	}
}

func WithPubRetain(retain bool) PublishOption {
	return func(opts *PublishOptions) {
		opts.Retain = retain
	}
}

func WithPubRoutingKeys(routingKeys ...string) PublishOption {
	return func(opts *PublishOptions) {
		opts.RoutingKeys = routingKeys
	}
}

func WithPubContentType(contentType string) PublishOption {
	return func(opts *PublishOptions) {
		opts.ContentType = contentType
	}
}
func WithPubDeliveryMode(deliveryMode uint8) PublishOption {
	return func(opts *PublishOptions) {
		opts.DeliveryMode = deliveryMode
	}
}
func WithPubExpiration(expiration string) PublishOption {
	return func(opts *PublishOptions) {
		opts.Expiration = expiration
	}
}
func WithPubContentEncoding(contentEncoding string) PublishOption {
	return func(opts *PublishOptions) {
		opts.ContentEncoding = contentEncoding
	}
}
func WithPubPriority(priority uint8) PublishOption {
	return func(opts *PublishOptions) {
		opts.Priority = priority
	}
}
func WithPubCorrelationID(correlationID string) PublishOption {
	return func(opts *PublishOptions) {
		opts.CorrelationID = correlationID
	}
}
func WithPubReplyTo(replyTo string) PublishOption {
	return func(opts *PublishOptions) {
		opts.ReplyTo = replyTo
	}
}
func WithPubMessageID(messageID string) PublishOption {
	return func(opts *PublishOptions) {
		opts.MessageID = messageID
	}
}
func WithPubTimestamp(timestamp time.Time) PublishOption {
	return func(opts *PublishOptions) {
		opts.Timestamp = timestamp
	}
}
func WithPubType(typ string) PublishOption {
	return func(opts *PublishOptions) {
		opts.Type = typ
	}
}
func WithPubUserID(userID string) PublishOption {
	return func(opts *PublishOptions) {
		opts.UserID = userID
	}
}
func WithPubAppID(appID string) PublishOption {
	return func(opts *PublishOptions) {
		opts.AppID = appID
	}
}

/*-----------------------------------------------------------------------------------------------------------*/

type SubscribeOptions struct {
	Qos            uint8             // QoS 0: 最多一次 1: 最少一次 2: 只一次 (仅MQTT)
	NackDiscard    bool              // 丢弃消息或进入死信队列(仅限RabbitMQ)
	QueueName      string            // 队列名称(仅限RabbitMQ)
	Block          bool              // 阻塞模式
	SubscriptionID int               // MQTT v5.0: Subscription identifier (1-268435455, 0 = none).
	UserProperties map[string]string // MQTT v5.0: User properties
}

type SubscribeOption func(opts *SubscribeOptions)

func WithSubBlock(block bool) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Block = block
	}
}

func WithSubSubscriptionID(subscriptionID int) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.SubscriptionID = subscriptionID
	}
}
func WithSubUserProperty(k, v string) SubscribeOption {
	return func(opts *SubscribeOptions) {
		if opts.UserProperties == nil {
			opts.UserProperties = make(map[string]string)
		}
		opts.UserProperties[k] = v
	}
}

func WithSubQueueName(queueName string) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.QueueName = queueName
	}
}

func WithSubQos(qos uint8) SubscribeOption {
	return func(opts *SubscribeOptions) {
		opts.Qos = qos
	}
}

func WithSubNackDiscard() SubscribeOption {
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

func WithMsgDeliveryMode(deliveryMode uint8) MessageOption {
	return func(opts *MessageOptions) {
		opts.DeliveryMode = deliveryMode
	}
}
func WithMsgPriority(priority uint8) MessageOption {
	return func(opts *MessageOptions) {
		opts.Priority = priority
	}
}
func WithMsgMessageId(messageId string) MessageOption {
	return func(opts *MessageOptions) {
		opts.MessageId = messageId
	}
}

func WithMsgTimestamp(timestamp time.Time) MessageOption {
	return func(opts *MessageOptions) {
		opts.Timestamp = timestamp
	}
}

func WithMsgType(typ string) MessageOption {
	return func(opts *MessageOptions) {
		opts.Type = typ
	}
}

func WithMsgUserID(userId string) MessageOption {
	return func(opts *MessageOptions) {
		opts.UserId = userId
	}
}
func WithMsgAppID(appId string) MessageOption {
	return func(opts *MessageOptions) {
		opts.AppId = appId
	}
}

func WithMsgReplyTo(replyTo string) MessageOption {
	return func(opts *MessageOptions) {
		opts.ReplyTo = replyTo
	}
}
func WithMsgExpiration(expiration string) MessageOption {
	return func(opts *MessageOptions) {
		opts.Expiration = expiration
	}
}

func WithMsgCorrelationID(correlationId string) MessageOption {
	return func(opts *MessageOptions) {
		opts.CorrelationId = correlationId
	}
}
