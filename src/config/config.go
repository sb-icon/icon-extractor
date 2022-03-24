package config

import (
	"log"

	"github.com/kelseyhightower/envconfig"
)

type configType struct {

	// General
	Name        string `envconfig:"NAME" required:"false" default:"icon-extractor"`
	NetworkName string `envconfig:"NETWORK_NAME" required:"false" default:"mainnnet"`

	// Extractors
	StartClaimExtractors    bool `envconfig:"START_CLAIM_EXTRACTORS" required:"false" default:"true"`
	NumClaimExtractors      int  `envconfig:"NUM_EXTRACTORS" required:"false" default:"4"`
	MaxClaimSize            int  `envconfig:"MAX_CLAIM_SIZE" required:"false" default:"1000"`
	StartHeadExtractor      bool `envconfig:"START_HEAD_EXTRACTOR" required:"false" default:"true"`
	HeadExtractorStartBlock int  `envconfig:"HEAD_EXTRACTOR_START_BLOCK" required:"false" default:"1"`

	// API
	APIPort           string `envconfig:"API_PORT" required:"false" default:"8000"`
	APIPrefix         string `envconfig:"API_PREFIX" required:"false" default:"/api/v1"`
	CORSAllowOrigins  string `envconfig:"CORS_ALLOW_ORIGINS" required:"false" default:"*"`
	CORSAllowHeaders  string `envconfig:"CORS_ALLOW_HEADERS" required:"false" default:"*"`
	CORSAllowMethods  string `envconfig:"CORS_ALLOW_METHODS" required:"false" default:"GET,POST,HEAD,PUT,DELETE,PATCH"`
	CORSExposeHeaders string `envconfig:"CORS_EXPOSE_HEADERS" required:"false" default:"*"`

	// Logging
	LogLevel         string `envconfig:"LOG_LEVEL" required:"false" default:"INFO"`
	LogToFile        bool   `envconfig:"LOG_TO_FILE" required:"false" default:"false"`
	LogFileName      string `envconfig:"LOG_FILE_NAME" required:"false" default:"etl.log"`
	LogFormat        string `envconfig:"LOG_FORMAT" required:"false" default:"console"`
	LogIsDevelopment bool   `envconfig:"LOG_IS_DEVELOPMENT" required:"false" default:"true"`

	// Icon node service
	IconNodeServiceURL          string `envconfig:"ICON_NODE_SERVICE_URL" required:"false" default:"https://api.icon.geometry.io/api/v3"`
	IconNodeServiceMaxBatchSize int    `envconfig:"ICON_NODE_SERVICE_MAX_BATCH_SIZE" required:"false" default:"100"`

	// Kafka
	KafkaBrokerURL        string `envconfig:"KAFKA_BROKER_URL" required:"false" default:"localhost:29092"`
	KafkaBlocksTopic      string `envconfig:"KAFKA_BLOCKS_TOPIC" required:"false" default:"icon-blocks"`
	KafkaBlocksPartitions int    `envconfig:"KAFKA_PRODUCER_PARTITIONS" required:"false" default:"12"`
	KafkaDeadMessageTopic string `envconfig:"KAFKA_DEAD_MESSAGE_TOPIC" required:"false" default:"icon-blocks-dead"`

	// DB
	DbDriver             string `envconfig:"DB_DRIVER" required:"false" default:"postgres"`
	DbHost               string `envconfig:"DB_HOST" required:"false" default:"localhost"`
	DbPort               string `envconfig:"DB_PORT" required:"false" default:"5432"`
	DbUser               string `envconfig:"DB_USER" required:"false" default:"postgres"`
	DbPassword           string `envconfig:"DB_PASSWORD" required:"false" default:"changeme"`
	DbName               string `envconfig:"DB_DBNAME" required:"false" default:"postgres"`
	DbSslmode            string `envconfig:"DB_SSL_MODE" required:"false" default:"disable"`
	DbTimezone           string `envconfig:"DB_TIMEZONE" required:"false" default:"UTC"`
	DbMaxIdleConnections int    `envconfig:"DB_MAX_IDLE_CONNECTIONS" required:"false" default:"2"`
	DbMaxOpenConnections int    `envconfig:"DB_MAX_OPEN_CONNECTIONS" required:"false" default:"10"`

	// GORM
	GormLoggingThresholdMilli int `envconfig:"GORM_LOGGING_THRESHOLD_MILLI" required:"false" default:"250"`
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
