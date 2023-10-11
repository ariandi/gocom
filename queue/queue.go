package queue

import (
	"fmt"
	"sync"

	"github.com/ariandi/gocom/config"
)

type QueueClient interface {
	Publish(name string, payload interface{}) error
	Consume(name string, consumer QueueConsumerFunc)
}

var queueMap map[string]QueueClient = map[string]QueueClient{}
var queueMutex sync.Mutex
var queueCreatorMap map[string]QueueCreatorFunc = map[string]QueueCreatorFunc{}

type QueueCreatorFunc func(url string) (QueueClient, error)
type QueueConsumerFunc func(name, payload string)

func RegQueueCreator(typeName string, creator QueueCreatorFunc) {

	queueCreatorMap[typeName] = creator
}

func Get(name ...string) QueueClient {

	targetName := "default"

	if len(name) > 0 {
		targetName = name[0]
	}

	ret, ok := queueMap[targetName]

	if !ok {

		queueMutex.Lock()
		defer queueMutex.Unlock()

		if config.HasConfig("app.queue."+targetName+".type") && config.HasConfig("app.queue."+targetName+".url") {

			queueType := config.Get("app.queue." + targetName + ".type")

			creator, ok := queueCreatorMap[queueType]

			if ok {

				url := config.Get("app.queue." + targetName + ".url")

				var err error
				ret, err = creator(url)

				if err == nil {

					queueMap[targetName] = ret

					fmt.Println("Conected to queue :", targetName)
				} else {

					fmt.Println("Error create queue :", err)
				}
			}
		}
	}

	return ret
}
