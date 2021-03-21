package kafka

import (
	"errors"
	"io"
	"sync"

	"github.com/Shopify/sarama"
)

type Client struct {
	client       sarama.Client
	config       sarama.Config
	producer     sarama.SyncProducer
	subcriptions sync.Map
}

type Subscribetionhandler interface {
	io.Closer
}

type singleConsumer struct {
	client sarama.Client
	signal chan struct{}
}

type SubscriptionHandler struct {
	kc       *Client
	name     string
	consumer io.Closer
}

type Consume struct {
	Topic   string
	Group   string
	Handler func(string, string, []byte) error
}

type groupConsumer struct {
	client sarama.Client
	signal chan struct{}
}

type consumerGroupHandler struct {
	group   string
	handler func(string, string, []byte) error
}

var (
	errMissingClient = errors.New("missing client kafka")
)
