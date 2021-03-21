package kafka

import (
	"log"

	"github.com/Shopify/sarama"
)

func Config() {

}

func WithCredential() {

}

func getKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_5_0_0
	config.Consumer.Return.Errors = true
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.RequiredAcks(-1)

	return config
}

func NewClint(address []string, config *sarama.Config) (*Client, error) {
	if config == nil {
		config = sarama.NewConfig()
	}

	config.Version = sarama.V2_5_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient(address, config)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: client,
	}, nil
}

func (c *Client) Close() error {
	if c.client == nil {
		return nil
	}

	func() {
		c.subcriptions.Range(func(key, value interface{}) bool {
			subsHandler, ok := value.(*SubscriptionHandler)
			if ok {
				_ = subsHandler.Close()
				return true
			}

			return false
		})
	}()

	if c.producer != nil {
		if err := c.producer.Close(); err != nil {
			log.Println("[error close producer]", err)
		}

		c.producer = nil
	}

	if err := c.client.Close(); err != nil {
		return err
	}

	c.client = nil

	return nil
}

func NewProducer(client sarama.Client) (sarama.SyncProducer, error) {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Println("[error init producer]", err)
		return nil, err
	}

	return producer, nil
}

func (c *Client) Publish(topic string, message []byte) error {
	var err error
	if c.producer == nil {
		c.producer, err = NewProducer(c.client)
		if err != nil {
			return err
		}
	}

	msg := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	if _, _, err = c.producer.SendMessage(&msg); err != nil {
		return err
	}

	return nil
}

func (c *Client) BulkPublish(topic string, message [][]byte) error {
	var err error
	if c.producer == nil {
		c.producer, err = NewProducer(c.client)
		if err != nil {
			return err
		}
	}

	var producerMessage []*sarama.ProducerMessage
	for _, msg := range message {
		producerMessage = append(producerMessage, &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(msg),
		})
	}

	if err = c.producer.SendMessages(producerMessage); err != nil {
		return err
	}

	return nil
}

func (c *Client) Subscribe(
	topic string,
	subscribetion string,
	handler func(string, string, []byte) error,
) (*Subscribetionhandler, error) {
	if c.client == nil {
		return nil, errMissingClient
	}

	consumer := newSingleConsumer(c.client)
	if len(subscribetion) > 0 {
		consumer = newSingleConsumer(c.client)
	}

	go func() {
		if err := consumer.Consumer(Consume{
			Topic:   topic,
			Group:   subscribetion,
			Handler: handler,
		}); err != nil {
			log.Println("[error consume kafka message]", err)
		}
	}()

	kch := &SubscriptionHandler{
		kc:       c,
		name:     topic,
		consumer: consumer,
	}

	c.addHandler(topic, kch)
	var sc Subscribetionhandler = kch
	return &sc, nil
}

func (ksh *SubscriptionHandler) Close() error {
	if ksh.consumer != nil {
		ksh.kc.removeHandler(ksh.name)
		return ksh.consumer.Close()
	}

	return nil
}

func (c *Client) addHandler(key string, handler *SubscriptionHandler) {
	c.subcriptions.Store(key, handler)
}

func (c *Client) removeHandler(key string) {
	c.subcriptions.Delete(key)
}
