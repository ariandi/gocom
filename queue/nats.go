package queue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

type NatsQueueClient struct {
	conn *nats.Conn
}

func (o *NatsQueueClient) Publish(name string, msg interface{}) error {

	var msgByte []byte
	var err error

	switch msg.(type) {
	case int, int16, int32, int64, string, float32, float64, bool:
		msgString := fmt.Sprintf("%v", msg)
		msgByte = []byte(msgString)
	default:
		msgByte, err = json.Marshal(msg)

		if err != nil {
			return err
		}
	}

	return o.conn.Publish(name, msgByte)
}

func (o *NatsQueueClient) PublishRaw(name string, msg []byte) error {

	return o.conn.Publish(name, msg)
}

func (o *NatsQueueClient) Consume(name string, consumer QueueConsumerFunc) {

	sub, err := o.conn.QueueSubscribe(name, name, func(msg *nats.Msg) {

		defer func() {

			err := recover()

			if err != nil {

				fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS QUEUE MSG :", name, ", Error :", err)
			}
		}()

		consumer(name, string(msg.Data))
	})

	if err == nil {

		no := 10000000
		sub.SetPendingLimits(no, no*1024)
	}
}

func (o *NatsQueueClient) ConsumeRaw(name string, consumer QueueRawConsumerFunc) {

	sub, err := o.conn.QueueSubscribe(name, name, func(msg *nats.Msg) {

		defer func() {

			err := recover()

			if err != nil {

				fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS QUEUE MSG :", name, ", Error :", err)
			}
		}()

		consumer(name, msg.Data)
	})

	if err == nil {

		no := 10000000
		sub.SetPendingLimits(no, no*1024)
	}
}

func (o *NatsQueueClient) Request(subject string, msg interface{}, timeOut ...time.Duration) (string, error) {

	var msgByte []byte
	var err error

	switch msg.(type) {
	case int, int16, int32, int64, string, float32, float64, bool:
		msgString := fmt.Sprintf("%v", msg)
		msgByte = []byte(msgString)
	default:
		msgByte, err = json.Marshal(msg)
		if err != nil {
			return "", err
		}
	}

	targetTimeout := 5 * time.Minute

	if len(timeOut) > 0 {
		targetTimeout = timeOut[0]
	}

	ret, err := o.conn.Request(subject, msgByte, targetTimeout)

	if err != nil {
		return "", err
	}

	return string(ret.Data), nil
}

func (o *NatsQueueClient) RequestRaw(subject string, msg []byte, timeOut ...time.Duration) ([]byte, error) {

	targetTimeout := 5 * time.Minute

	if len(timeOut) > 0 {
		targetTimeout = timeOut[0]
	}

	ret, err := o.conn.Request(subject, msg, targetTimeout)

	if err != nil {
		return nil, err
	}

	return ret.Data, nil
}

func (o *NatsQueueClient) Reply(subject string, eventHandler QueueReqHandler) {

	sub, err := o.conn.QueueSubscribe(subject, subject, func(msg *nats.Msg) {

		defer func() {

			err := recover()

			if err != nil {

				fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS REPLY MSG :", subject, " : ", err)
			}
		}()

		ret := eventHandler(subject, string(msg.Data))
		msg.Respond([]byte(ret))
	})

	if err == nil {

		no := 10000000
		sub.SetPendingLimits(no, no*1024)
	}
}

func (o *NatsQueueClient) ReplyRaw(subject string, eventHandler QueueRawReqHandler) {

	sub, err := o.conn.QueueSubscribe(subject, subject, func(msg *nats.Msg) {

		defer func() {

			err := recover()

			if err != nil {

				fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS REPLY MSG :", subject, " : ", err)
			}
		}()

		ret := eventHandler(subject, msg.Data)
		msg.Respond(ret)
	})

	if err == nil {

		no := 10000000
		sub.SetPendingLimits(no, no*1024)
	}
}

func init() {
	RegQueueCreator("nats", func(url string) (QueueClient, error) {

		ret := &NatsQueueClient{}

		var err error
		ret.conn, err = nats.Connect(url)

		if err != nil {
			return nil, err
		}

		return ret, nil
	})
}
