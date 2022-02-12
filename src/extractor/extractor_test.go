package extractor

import (
	"encoding/json"
	"testing"

	"github.com/geometry-labs/icon-go-etl/config"
	"github.com/geometry-labs/icon-go-etl/logging"
	"github.com/geometry-labs/icon-go-etl/service"
	"github.com/stretchr/testify/assert"
)

// Runs before every test
func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestStart(t *testing.T) {
	assert := assert.New(t)

	// Input Channels
	extractorQueueChannel := make(chan int64)

	// Output channels
	extractorCommitChannel := make(chan int64)
	blockOutputChannel := make(chan service.IconNodeResponseGetBlockByHeight)

	extractor := Extractor{
		blockNumberQueue:  extractorQueueChannel,
		blockNumberCommit: extractorCommitChannel,
		blockOutput:       blockOutputChannel,
	}
	extractor.Start()

	go func() {
		blockNumber := int64(1)

		for {
			extractorQueueChannel <- blockNumber

			_ = <-extractorCommitChannel

			blockNumber++
		}
	}()

	for i := 0; i < 10; i++ {

		block := <-blockOutputChannel

		b, err := json.Marshal(&block)
		assert.Equal(nil, err)
		t.Logf(string(b))
	}
}
