package extractor

import (
	"testing"

	"github.com/geometry-labs/icon-go-etl/config"
	"github.com/geometry-labs/icon-go-etl/logging"
	"github.com/geometry-labs/icon-go-etl/service"
	"github.com/geometry-labs/icon-go-etl/transformer"
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

	jobQueueChannel := make(chan ExtractorJob)
	jobCommitChannel := make(chan ExtractorJob)
	blockOutputChannel := make(chan service.IconNodeResponseGetBlockByHeightResult)

	extractor := Extractor{
		jobQueue:    jobQueueChannel,
		jobCommit:   jobCommitChannel,
		blockOutput: transformer.RawBlockChannel,
	}
	extractor.Start()

	startBlockNumber := int64(1)
	endBlockNumber := int64(10)
	jobQueueChannel <- ExtractorJob{
		startBlockNumber: startBlockNumber,
		endBlockNumber:   endBlockNumber,
	}

	go func() {
		for {
			_ = <-jobCommitChannel
		}
	}()

	for i := int64(0); i < endBlockNumber-startBlockNumber; i++ {

		block := <-blockOutputChannel

		// Assert values in block
		assert.Equal(startBlockNumber+i, block.Height)
	}
}
