package mqc

import (
	"fmt"
	"strings"

	"github.com/civet148/go-mqc/mqtt"
	"github.com/civet148/go-mqc/options"
	"github.com/civet148/go-mqc/rabbit"
	"github.com/civet148/go-mqc/types"
)

const (
	nameMQTT     = "mqtt"
	nameRabbitMQ = "rabbitmq"
)

const (
	schemaMQTT     = "mqtt"
	schemaTcp      = "tcp"
	schemaAMQP     = "amqp"
	schemaRabbit   = "rabbit"
	schemaRabbitMQ = "rabbitmq"
)

func init() {
	register(nameMQTT, mqtt.NewClient)
	register(nameRabbitMQ, rabbit.NewClient)
}

type instanceFunc func(address string, opfs ...options.DialOption) (types.MQ, error)

var instances = map[string]instanceFunc{}

func register(name string, newer instanceFunc) {
	instances[name] = newer
}

func getClientName(schema string) string {
	switch schema {
	case schemaRabbitMQ, schemaAMQP, schemaRabbit:
		return nameRabbitMQ
	default:
		return nameMQTT
	}
}

func NewMQ(address string, opfs ...options.DialOption) (types.MQ, error) {
	ui, err := parseUrl(address)
	if err != nil {
		return nil, fmt.Errorf("invalid dial address [%v]", address)
	}
	clientName := getClientName(ui.Scheme)
	newer, ok := instances[clientName]
	if !ok {
		return nil, fmt.Errorf("unsupported client name [%v]", clientName)
	}
	address = replaceAddress(clientName, address)
	return newer(address, opfs...)
}

func replaceAddress(schema, address string) string {
	switch schema {
	case schemaMQTT:
		return strings.Replace(address, "mqtt://", "tcp://", 1)
	case schemaRabbitMQ:
		return strings.Replace(address, "rabbitmq://", "amqp://", 1)
	}
	return address
}
