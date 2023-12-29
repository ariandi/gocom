package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/adjust/rmq/v5"
	"github.com/redis/go-redis/v9"
)

// Queue ------------------------------------------------------------------------------

type RedisQueue struct {
	conn  rmq.Connection
	queue map[string]rmq.Queue
}

func (o *RedisQueue) Publish(name string, payload interface{}) error {

	queue, ok := o.queue[name]

	if !ok {

		var err error
		queue, err = o.conn.OpenQueue("name")

		if err != nil {
			return err
		}

		o.queue[name] = queue
	}

	payloadString := ""

	switch payload.(type) {
	case int, int16, int32, int64, string, float32, float64, bool:
		payloadString = fmt.Sprintf("%v", payload)
	default:
		payloadByte, err := json.Marshal(payload)

		if err != nil {
			return err
		}

		payloadString = string(payloadByte)
	}

	queue.Publish(payloadString)
	return nil
}

func (o *RedisQueue) PublishRaw(name string, payload []byte) error {
	return errors.New("Not implemented")
}

func (o *RedisQueue) Consume(name string, consumer QueueConsumerFunc) {
}

func (o *RedisQueue) ConsumeRaw(name string, consumer QueueRawConsumerFunc) {
}

func (o *RedisQueue) Request(subject string, msg interface{}, timeOut ...time.Duration) (string, error) {

	return "", errors.New("Not implemented")
}

func (o *RedisQueue) Reply(subject string, eventHandler QueueReqHandler) {

}

func (o *RedisQueue) RequestRaw(subject string, msg []byte, timeOut ...time.Duration) ([]byte, error) {

	return nil, errors.New("Not implemented")
}

func (o *RedisQueue) ReplyRaw(subject string, eventHandler QueueRawReqHandler) {

}

// Init -------------------------------------------------------------------------------

func init() {
	RegQueueCreator("redis", func(url string) (QueueClient, error) {

		ret := &RedisQueue{
			queue: map[string]rmq.Queue{},
		}

		opt, err := redis.ParseURL(url)

		if err != nil {
			return nil, fmt.Errorf("unable to parse redis url %w", err)
		}

		ret.conn, err = rmq.OpenConnection("queue", "tcp", opt.Addr, opt.DB, nil)

		if err != nil {
			return nil, fmt.Errorf("unable to connect redis %w", err)
		}

		return ret, nil
	})
}
