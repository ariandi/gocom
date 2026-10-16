package pubsub

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ariandi/gocom/internal/natsconn"
	"github.com/nats-io/nats.go"
)

type NatsPubSubClient struct {
	conn *nats.Conn
}

func (o *NatsPubSubClient) Publish(subject string, msg interface{}) error {

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

	return o.conn.Publish(subject, msgByte)
}

func (o *NatsPubSubClient) Request(subject string, msg interface{}, timeOut ...time.Duration) (string, error) {

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

	targetTimeout := 20 * time.Second

	if len(timeOut) > 0 {
		targetTimeout = timeOut[0]
	}

	ret, err := o.conn.Request(subject, msgByte, targetTimeout)

	if err != nil {
		return "", err
	}

	return string(ret.Data), nil
}

func (o *NatsPubSubClient) Subscribe(subject string, eventHandler PubSubEventHandler) {

	sub, err := o.conn.Subscribe(subject, func(msg *nats.Msg) {

		defer func() {

			err := recover()

			if err != nil {

				fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS MSG :", subject, " : ", err)
			}
		}()

		eventHandler(subject, string(msg.Data))
	})

	if err == nil {

		no := 10000000
		sub.SetPendingLimits(no, no*1024)
	}
}

func (o *NatsPubSubClient) RequestSubscribe(subject string, eventHandler PubSubReqEventHandler) {

	sub, err := o.conn.Subscribe(subject, func(msg *nats.Msg) {

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

func (o *NatsPubSubClient) QueueSubscribe(subject string, queue string, eventHandler PubSubEventHandler) {

	sub, err := o.conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {

		defer func() {

			err := recover()

			if err != nil {

				fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS QUEUE MSG :", subject, ", Queue : ", queue, ", Error :", err)
			}
		}()

		eventHandler(subject, string(msg.Data))
	})

	if err == nil {

		no := 10000000
		sub.SetPendingLimits(no, no*1024)
	}
}

// appNatsConfig supplies the optional app.nats.* tuning to the registered creator.
// A variable so tests can substitute it: the config package only reads files at startup.
var appNatsConfig = natsconn.ConfigFromApp

func init() {
	RegPubSubCreator("nats", func(connString string) (PubSubClient, error) {
		return newNatsPubSubClient(connString, appNatsConfig())
	})
}

// newNatsPubSubClient returns a nil interface (not a typed nil) on failure, so
// `if pubsub.Get() == nil` startup guards in callers keep working.
//
// A bare nats.Connect used to live here: after ~2 minutes of broker downtime it
// left the client CLOSED for good, while Get() kept handing it out. See natsconn.
func newNatsPubSubClient(connString string, cfg natsconn.Config) (PubSubClient, error) {
	conn, err := natsconn.Connect("pubsub", connString, cfg)
	if err != nil {
		return nil, err
	}

	return &NatsPubSubClient{conn: conn}, nil
}
