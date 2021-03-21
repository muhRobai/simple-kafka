package kafka

import (
	"log"

	"github.com/Shopify/sarama"
)

const (
	defaultZeroNumber = 0
)

// newSingleConsumer : single consumer kafka
func newSingleConsumer(client sarama.Client) *singleConsumer {
	return &singleConsumer{
		client: client,
		signal: make(chan struct{}, 1),
	}
}

func (c *singleConsumer) Consumer(item Consume) error {
	consumer, err := sarama.NewConsumerFromClient(c.client)
	if err != nil {
		log.Println("[error initial consumer]", err)
		return err
	}

	partitionNameConsumer, err := consumer.ConsumePartition(item.Topic, defaultZeroNumber, sarama.OffsetNewest)
	if err != nil {
		log.Println("[error partitional name consumer]", err)
		return err
	}

loop:
	for {
		select {
		case msg := <-partitionNameConsumer.Messages():
			if err := item.Handler(msg.Topic, item.Group, msg.Value); err != nil {
				log.Println("[error process messgae]", err)
			}
		case <-c.signal:
			break loop
		}

	}

	if err := partitionNameConsumer.Close(); err != nil {
		log.Println("[error close partition consumer]", err)
		return err
	}

	return consumer.Close()
}

func (c *singleConsumer) Close() error {
	close(c.signal)
	return nil
}
