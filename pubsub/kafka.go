package pubsub

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type KafkaPubSubClient struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
}

func (o *KafkaPubSubClient) Publish(subject string, msg interface{}) error {

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

	topic := "test-kafka"

	return o.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(subject),
		Value:          msgByte,
	}, nil)
}

func (o *KafkaPubSubClient) Request(subject string, msg interface{}, timeOut ...time.Duration) (string, error) {

	//var msgByte []byte
	//var err error
	//
	//switch msg.(type) {
	//case int, int16, int32, int64, string, float32, float64, bool:
	//	msgString := fmt.Sprintf("%v", msg)
	//	msgByte = []byte(msgString)
	//default:
	//	msgByte, err = json.Marshal(msg)
	//	if err != nil {
	//		return "", err
	//	}
	//}
	//
	//targetTimeout := 20 * time.Second
	//
	//if len(timeOut) > 0 {
	//	targetTimeout = timeOut[0]
	//}
	//
	//ret, err := o.conn.Request(subject, msgByte, targetTimeout)
	//
	//if err != nil {
	//	return "", err
	//}
	//
	//return string(ret.Data), nil
	return "", nil
}

func (o *KafkaPubSubClient) Subscribe(subject string, eventHandler PubSubEventHandler) {

	//sub, err := o.conn.Subscribe(subject, func(msg *nats.Msg) {
	//
	//	defer func() {
	//
	//		err := recover()
	//
	//		if err != nil {
	//
	//			fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS MSG :", subject, " : ", err)
	//		}
	//	}()
	//
	//	eventHandler(subject, string(msg.Data))
	//})
	//
	//if err == nil {
	//
	//	no := 10000000
	//	sub.SetPendingLimits(no, no*1024)
	//}
}

func (o *KafkaPubSubClient) RequestSubscribe(subject string, eventHandler PubSubReqEventHandler) {

	//sub, err := o.conn.Subscribe(subject, func(msg *nats.Msg) {
	//
	//	defer func() {
	//
	//		err := recover()
	//
	//		if err != nil {
	//
	//			fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS REPLY MSG :", subject, " : ", err)
	//		}
	//	}()
	//
	//	ret := eventHandler(subject, string(msg.Data))
	//	msg.Respond([]byte(ret))
	//})
	//
	//if err == nil {
	//
	//	no := 10000000
	//	sub.SetPendingLimits(no, no*1024)
	//}
}

func (o *KafkaPubSubClient) QueueSubscribe(subject string, queue string, eventHandler PubSubEventHandler) {

	//sub, err := o.conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
	//
	//	defer func() {
	//
	//		err := recover()
	//
	//		if err != nil {
	//
	//			fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS QUEUE MSG :", subject, ", Queue : ", queue, ", Error :", err)
	//		}
	//	}()
	//
	//	eventHandler(subject, string(msg.Data))
	//})
	//
	//if err == nil {
	//
	//	no := 10000000
	//	sub.SetPendingLimits(no, no*1024)
	//}
}

func init() {
	RegPubSubCreator("kafka", func(connString string) (PubSubClient, error) {
		ret := &KafkaPubSubClient{}

		var err error
		// ret.conn, err = nats.Connect(connString)
		config := kafka.ConfigMap{
			"bootstrap.servers": connString, // Replace with your Kafka broker(s)
		}

		ret.producer, err = kafka.NewProducer(&config)
		if err != nil {
			return nil, err
		}

		return ret, nil
	})
}
