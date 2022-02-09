package transformer

import "go.uber.org/zap"

type RawMessage struct {
	Block        interface{}
	Transactions []interface{}
}

var RawMessageChannel chan RawMessage

func StartTransformer() {

	RawMessageChannel = make(chan RawMessage)

	go startTransformer()
}

func startTransformer() {

	for {

		/////////////////
		// Raw Message //
		/////////////////

		rawMessage := <-RawMessageChannel

		zap.S().Info(rawMessage)
	}
}
