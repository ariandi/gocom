package pubsub

import (
	"fmt"
	"sync"
	"time"

	"github.com/ariandi/gocom/config"
)

var pubSubMap map[string]PubSubClient = map[string]PubSubClient{}
var pubSubMutex sync.Mutex
var pubSubCreatorMap map[string]PubSubCreatorFunc = map[string]PubSubCreatorFunc{}

type PubSubClient interface {
	Publish(subject string, msg interface{}) error
	Request(subject string, msg interface{}, timeOut ...time.Duration) (string, error)
	Subscribe(subject string, eventHandler PubSubEventHandler)
	RequestSubscribe(subject string, eventHandler PubSubReqEventHandler)
	QueueSubscribe(subject string, queue string, eventHandler PubSubEventHandler)
}

type PubSubCreatorFunc func(connString string) (PubSubClient, error)
type PubSubEventHandler func(name string, msg string)
type PubSubReqEventHandler func(name string, msg string) string

func RegPubSubCreator(typeName string, creator PubSubCreatorFunc) {

	pubSubCreatorMap[typeName] = creator
}

func Get(name ...string) PubSubClient {

	targetName := "default"

	if len(name) > 0 {
		targetName = name[0]
	}

	if targetName == "" {
		targetName = "default"
	}

	ret, ok := pubSubMap[targetName]

	if !ok {
		pubSubMutex.Lock()
		defer pubSubMutex.Unlock()

		if config.HasConfig("app.pubsub."+targetName+".type") && config.HasConfig("app.pubsub."+targetName+".url") {

			pubSubType := config.Get("app.pubsub." + targetName + ".type")

			creator, ok := pubSubCreatorMap[pubSubType]

			if ok {

				url := config.Get("app.pubsub." + targetName + ".url")

				var err error
				ret, err = creator(url)

				if err == nil {

					pubSubMap[targetName] = ret

					fmt.Println("Conected to PubSub :", targetName)
				} else {

					fmt.Println("Error create PubSub :", err)
				}
			}
		}
	}

	return ret
}
