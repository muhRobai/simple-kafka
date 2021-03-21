package kafka

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
)

func newGroupConsumer(client sarama.Client) *groupConsumer {
	return &groupConsumer{
		client: client,
		signal: make(chan struct{}, 1),
	}
}

func (c *groupConsumer) Consumer(item Consume) error {
	consumer, err := sarama.NewConsumerGroupFromClient(item.Group, c.client)
	if err != nil {
		return err
	}

	go func() {
		for err := range consumer.Errors() {
			log.Println("[error group consumer]", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topics := []string{item.Topic}
	cgHandler := consumerGroupHandler{
		group:   item.Group,
		handler: item.Handler,
	}

	go func() {
		for {
			if err := consumer.Consume(ctx, topics, cgHandler); err != nil {
				log.Println("[error consume group]", err)
				return
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-c.signal
	return consumer.Close()
}

func (g consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (g consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (g consumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		if err := g.handler(msg.Topic, g.group, msg.Value); err != nil {
			log.Println("error while running subscribetion callback method", err)
		} else {
			session.MarkMessage(msg, "")
		}
	}

	return nil
}
