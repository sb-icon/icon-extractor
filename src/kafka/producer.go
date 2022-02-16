package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/geometry-labs/icon-go-etl/config"
)

type KafkaTopicProducer struct {
	BrokerURL string
	TopicName string
	TopicChan chan *sarama.ProducerMessage
}

// map[Topic_Name] -> Producer
var KafkaTopicProducers = map[string]*KafkaTopicProducer{}

func StartProducers() {
	kafkaBroker := config.Config.KafkaBrokerURL
	blocksTopic := config.Config.KafkaBlocksTopic
	deadMessageTopic := config.Config.KafkaDeadMessageTopic

	KafkaTopicProducers[blocksTopic] = &KafkaTopicProducer{
		kafkaBroker,
		blocksTopic,
		make(chan *sarama.ProducerMessage),
	}

	KafkaTopicProducers[deadMessageTopic] = &KafkaTopicProducer{
		kafkaBroker,
		deadMessageTopic,
		make(chan *sarama.ProducerMessage),
	}

	go KafkaTopicProducers[blocksTopic].produceTopic()
	go KafkaTopicProducers[deadMessageTopic].produceTopic()
}

func (k *KafkaTopicProducer) produceTopic() {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := getProducer(k, config)
	if err != nil {
		zap.S().Warn("KAFKA PRODUCER ERROR: Finally Connection cannot be established")
		return
	}

	defer func() {
		if err := producer.Close(); err != nil {
			zap.S().Warn(": ", err.Error())
		}
	}()

	for {
		topicMessage := <-k.TopicChan

		partition, offset, err := producer.SendMessage(topicMessage)
		if err != nil {
			zap.S().Warn("Producer ", k.TopicName, ": Err sending message=", err.Error())
		}

		zap.S().Info("Topic=", k.TopicName, " Partition=", partition, " offset=", offset, " - Produced message to kafka")
	}
}

func getProducer(k *KafkaTopicProducer, config *sarama.Config) (sarama.SyncProducer, error) {
	var producer sarama.SyncProducer
	operation := func() error {
		pro, err := sarama.NewSyncProducer([]string{k.BrokerURL}, config)
		if err != nil {
			zap.S().Info("KAFKA PRODUCER NEWSYNCPRODUCER PANIC: ", err.Error())
		} else {
			producer = pro
		}
		return err
	}
	neb := backoff.NewExponentialBackOff()
	neb.MaxElapsedTime = time.Minute
	err := backoff.Retry(operation, neb)
	return producer, err
}
