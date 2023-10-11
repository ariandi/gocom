package gocom

import (
	"github.com/ariandi/gocom/keyval"
	"github.com/ariandi/gocom/pubsub"
	"github.com/ariandi/gocom/queue"
)

func KeyVal(name ...string) keyval.KeyValClient {
	return keyval.Get(name...)
}

func PubSub(name ...string) pubsub.PubSubClient {
	return pubsub.Get(name...)
}

func Queue(name ...string) queue.QueueClient {
	return queue.Get(name...)
}
