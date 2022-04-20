package extractor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sudoblockio/icon-extractor/config"
	"github.com/sudoblockio/icon-extractor/logging"
	"github.com/sudoblockio/icon-extractor/service"
)

// Runs before every test
func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestStartHead(t *testing.T) {
	assert := assert.New(t)

	config.ReadEnvironment()

	config.Config.IconNodeServiceMaxBatchSize = 5
	config.Config.MaxClaimSize = 5

	blockOutputChannel := make(chan service.IconNodeResponseGetBlockByHeightResult)

	e := Extractor{
		blockOutputChannel,
	}
	e.start(true)

	block := <-blockOutputChannel

	// Assert values in block
	assert.NotEqual(0, block.Height)
}
