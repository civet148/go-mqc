package types

import (
	"context"

	"github.com/civet148/go-mqc/options"
)

type MQ interface {
	Publish(ctx context.Context, topic string, msg any, opfs ...options.PublishOption) error
	Subscribe(ctx context.Context, topic string, handler MessageHandler, opfs ...options.SubscribeOption) error
	Close(ctx context.Context) error
}

type MessageHandler func(topic string, data any, opfs ...options.MessageOption) error
