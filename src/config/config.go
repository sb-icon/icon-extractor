package config

import (
	"log"

	"github.com/kelseyhightower/envconfig"
)

type configType struct {
	Name        string `envconfig:"NAME" required:"false" default:"icon-go-etl"`
	NetworkName string `envconfig:"NETWORK_NAME" required:"false" default:"mainnnet"`

	// Logging
	LogLevel         string `envconfig:"LOG_LEVEL" required:"false" default:"INFO"`
	LogToFile        bool   `envconfig:"LOG_TO_FILE" required:"false" default:"false"`
	LogFileName      string `envconfig:"LOG_FILE_NAME" required:"false" default:"etl.log"`
	LogFormat        string `envconfig:"LOG_FORMAT" required:"false" default:"json"`
	LogIsDevelopment bool   `envconfig:"LOG_IS_DEVELOPMENT" required:"false" default:"true"`

	// Icon node service
	IconNodeServiceURL          string `envconfig:"ICON_NODE_SERVICE_URL" required:"false" default:"https://ctz.solidwallet.io/api/v3"`
	IconNodeServiceMaxBatchSize int    `envconfig:"ICON_NODE_SERVICE_MAX_BATCH_SIZE" required:"false" default:"10"`

	// Kafka
	KafkaBrokerURL     string `envconfig:"KAFKA_BROKER_URL" required:"false" default:"localhost:29092"`
	KafkaProducerTopic string `envconfig:"KAFKA_PRODUCER_TOPIC" required:"false" default:"icon-blocks"`
}

// Config - runtime config struct
var Config configType

// ReadEnvironment - Read and store runtime config
func ReadEnvironment() {
	err := envconfig.Process("", &Config)
	if err != nil {
		log.Fatalf("ERROR: envconfig - %s\n", err.Error())
	}
}
