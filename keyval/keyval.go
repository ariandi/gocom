package keyval

import (
	"fmt"
	"sync"
	"time"

	"github.com/ariandi/gocom/config"
)

// --------------------------

type KeyValClient interface {
	Set(key string, val interface{}, ttl ...time.Duration) error
	SetNX(key string, val interface{}, ttl ...time.Duration) bool
	Get(key string) string
	GetInt(key string) int
	Del(key string) error
	GetObject(key string, obj interface{}) error

	Incr(key string) int64
	Decr(key string) int64

	LPush(key string, val interface{}) error
	LPop(key string) string
	LPopCount(key string, count int) []string
	LPopInt(key string) int

	RPush(key string, val interface{}) error
	RPop(key string) string
	RPopCount(key string, count int) []string
	RPopInt(key string) int

	Len(key string) int64
	AtIndex(key string, index int64) string
	AtIndexInt(key string, index int64) int
	Range(key string, start int64, stop int64) []string

	HSet(key string, values map[string]interface{}) error
	HSetNX(key string, values map[string]interface{}) error
	HGet(key, field string) string
	HGetAll(key string) map[string]string
	HDel(key string, fields ...string) error
	HLen(key string) int
	HScan(key, pattern string, from, count int) map[string]string

	Expire(key string, ttl time.Duration) error
	Exist(key string) bool
}

// --------------------------

var keyValMap map[string]KeyValClient = map[string]KeyValClient{}
var keyValMutex sync.Mutex
var keyValCreatorMap map[string]KeyValCreatorFunc = map[string]KeyValCreatorFunc{}

type KeyValCreatorFunc func(url string) (KeyValClient, error)

func RegKeyValCreator(typeName string, creator KeyValCreatorFunc) {

	keyValCreatorMap[typeName] = creator
}

func Get(name ...string) KeyValClient {

	targetName := "default"

	if len(name) > 0 {
		targetName = name[0]
	}

	ret, ok := keyValMap[targetName]

	if !ok {

		keyValMutex.Lock()
		defer keyValMutex.Unlock()

		if config.HasConfig("app.keyval."+targetName+".type") && config.HasConfig("app.keyval."+targetName+".url") {

			keyValType := config.Get("app.keyval." + targetName + ".type")

			creator, ok := keyValCreatorMap[keyValType]

			if ok {

				url := config.Get("app.keyval." + targetName + ".url")

				var err error
				ret, err = creator(url)

				if err == nil {

					keyValMap[targetName] = ret

					fmt.Println("Conected to KeyVal :", targetName)
				} else {

					fmt.Println("Error create KeyVal :", err)
				}
			}
		}
	}

	return ret
}
