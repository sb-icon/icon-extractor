package tests

import (
	"bytes"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

// Config required:
//   - START_CLAIM_EXTRACTORS: true
//   - NUM_CLAIM_EXTRACTORS: >0
//   - MAX_CLAIM_SIZE: >0
//   - API_PORT: 8000
//   - API_PREFIX: /api/v1
var blockVersions = []struct {
	number  int
	version string
}{
	{10324748, "0.1a"},
	{12640760, "0.3"},
	{14473621, "0.4"},
	{14473622, "0.5"},
}

func TestBlockVersions(t *testing.T) {
	assert := assert.New(t)

	/////////////////
	// Request Job //
	/////////////////
	for _, blockVersion := range blockVersions {
		serviceURL := "http://localhost:8000/api/v1/create-job"

		// Get latest block
		body := []byte(fmt.Sprintf(`{
			"start_block_number": %d,
			"end_block_number": %d
		}`, blockVersion.number, blockVersion.number+1))
		resp, err := http.Post(serviceURL, "*/*", bytes.NewBuffer(body))
		assert.Equal(nil, err)
		assert.Equal(200, resp.StatusCode)

		defer resp.Body.Close()
	}

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

	// block numbers to read
	blockNumbers := map[int]int{}
	for i, blockVersion := range blockVersions {
		blockNumbers[blockVersion.number] = i
	}

	c.SubscribeTopics([]string{"icon-blocks"}, nil)
	i := 0
	for {
		msg, err := c.ReadMessage(-1)
		assert.Equal(nil, err)
		assert.NotEqual(nil, msg)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Key))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}

		// TODO verify blocks in blockVersions
		break

		keys := reflect.ValueOf(blockNumbers).MapKeys()
		if len(key) == 0 {
			break
		}
	}

	c.Close()
}
