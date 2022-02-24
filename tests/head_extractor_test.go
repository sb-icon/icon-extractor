package tests

import (
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

// Config required:
//   - START_HEAD_EXTRACTOR: true
func TestHeadExtractors(t *testing.T) {
	assert := assert.New(t)

	/////////////
	// Consume //
	/////////////
	// https://github.com/confluentinc/confluent-kafka-go#examples
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:29092",
		"group.id":          "integration-test-group",
		"auto.offset.reset": "earliest",
	})

	assert.Equal(nil, err)

	c.SubscribeTopics([]string{"icon-blocks"}, nil)
	i := 0
	for {
		msg, err := c.ReadMessage(-1)
		assert.Equal(nil, err)
		assert.NotEqual(nil, msg)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}

		if i == 10 {
			// Verify 10 messages
			break
		}
		i++
	}

	c.Close()
}
