package kafka

import (
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/sb-icon/icon-extractor/config"
)

type KafkaTopicProducer struct {
	BrokerURL       string
	TopicName       string
	TopicPartitions int
	TopicChan       chan *sarama.ProducerMessage
}

// map[TopicName] -> Producer
var KafkaTopicProducers = map[string]*KafkaTopicProducer{}

func StartProducers() {
	kafkaBroker := config.Config.KafkaBrokerURL
	blocksTopic := config.Config.KafkaBlocksTopic
	blocksPartitions := config.Config.KafkaBlocksPartitions
	deadMessageTopic := config.Config.KafkaDeadMessageTopic

	KafkaTopicProducers[blocksTopic] = &KafkaTopicProducer{
		kafkaBroker,
		blocksTopic,
		blocksPartitions,
		make(chan *sarama.ProducerMessage),
	}

	KafkaTopicProducers[deadMessageTopic] = &KafkaTopicProducer{
		kafkaBroker,
		deadMessageTopic,
		blocksPartitions,
		make(chan *sarama.ProducerMessage),
	}

	go KafkaTopicProducers[blocksTopic].produceTopic()
	go KafkaTopicProducers[deadMessageTopic].produceTopic()
}

func (k *KafkaTopicProducer) produceTopic() {
	saramaConfig := sarama.NewConfig()

	//////////////////
	// Create topic //
	//////////////////
	admin, err := getAdmin(k.BrokerURL, saramaConfig)
	if err != nil {
		zap.S().Fatal("KAFKA ADMIN ERROR: ", err.Error())
	}
	defer func() { _ = admin.Close() }()

	// check if topic is already made
	topics, err := admin.ListTopics()
	if _, ok := topics[k.TopicName]; ok == false {
		zap.S().Info("Creating topics: ", k.TopicName)
		// Create topic
		err = admin.CreateTopic(k.TopicName, &sarama.TopicDetail{
			NumPartitions:     int32(k.TopicPartitions),
			ReplicationFactor: int16(config.Config.KafkaReplicationFactor),

			// Can add and modify many configs for topic, refer to:
			// https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html
			ConfigEntries: map[string]*string{
				"compression.type":  &config.Config.KafkaCompressionType,
				"max.message.bytes": &config.Config.KafkaMaxMessageBytes,
				"cleanup.policy":    &config.Config.KafkaCleanupPolicy,
				"retention.bytes":   &config.Config.KafkaRetentionBytes,
				"retention.ms":      &config.Config.KafkaRetentionMs,
			},
		}, false)
		if err != nil {
			zap.S().Warn("Error while creating topic: ", err.Error())
		}
	}

	/////////////////////
	// Create producer //
	/////////////////////
	saramaConfig.Producer.MaxMessageBytes, err = strconv.Atoi(config.Config.KafkaMaxMessageBytes)
	if err != nil {
		zap.S().Fatal(err)
	}
	saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	producer, err := getProducer(k.BrokerURL, saramaConfig)
	if err != nil {
		zap.S().Fatal("KAFKA PRODUCER ERROR: Finally Connection cannot be established")
	}
	defer func() { _ = producer.Close() }()

	for {
		topicMessage := <-k.TopicChan

		partition, offset, err := producer.SendMessage(topicMessage)

		topicMessageKey, _ := topicMessage.Key.Encode()
		if err != nil {
			zap.S().Warn("Topic=", k.TopicName, " Partition=", partition, " offset=", offset, " key=", string(topicMessageKey), " - Error: ", err.Error())
			//// TODO: Create table in PG and push bad blocks
			//problemBlock := &models.ProblemBlock{}
			//problemBlock.BlockNumber = topicMessage
			//crud.GetProblemBlockCrud().LoaderChannel <- problemBlock
		}
		zap.S().Debug("Topic=", k.TopicName, " Partition=", partition, " offset=", offset, " key=", string(topicMessageKey), " - Produced message to kafka")
	}
}

func getAdmin(brokerURL string, saramaConfig *sarama.Config) (sarama.ClusterAdmin, error) {
	var admin sarama.ClusterAdmin
	operation := func() error {
		a, err := sarama.NewClusterAdmin([]string{brokerURL}, saramaConfig)
		if err != nil {
			zap.S().Warn("KAFKA ADMIN NEWCLUSTERADMIN WARN: ", err.Error())
		} else {
			admin = a
		}
		return err
	}
	neb := backoff.NewConstantBackOff(time.Second)
	err := backoff.Retry(operation, neb)
	return admin, err
}

func getProducer(brokerURL string, saramaConfig *sarama.Config) (sarama.SyncProducer, error) {
	var producer sarama.SyncProducer
	operation := func() error {
		pro, err := sarama.NewSyncProducer([]string{brokerURL}, saramaConfig)
		if err != nil {
			zap.S().Warn("KAFKA PRODUCER NEWSYNCPRODUCER WARN: ", err.Error())
		} else {
			producer = pro
		}
		return err
	}
	neb := backoff.NewConstantBackOff(time.Second)
	err := backoff.Retry(operation, neb)
	return producer, err
}
