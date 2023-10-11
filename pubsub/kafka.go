package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type KafkaPubSubClient struct {
	producer  *kafka.Producer
	consumer  *kafka.Consumer
	configMap *kafka.ConfigMap
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
	topic := "chat_bot"

	// check the topic is existed or not // if not exist will create new topic
	o.checkTopic(topic)

	return o.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(subject),
		Value:          msgByte,
	}, nil)
}

func (o *KafkaPubSubClient) Request(subject string, msg interface{}, timeOut ...time.Duration) (string, error) {
	return "", nil
}

func (o *KafkaPubSubClient) Subscribe(subject string, eventHandler PubSubEventHandler) {
	topic := "chat_bot"
	err := o.consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Error subscribing to topic: %v\n", err)
		defer func() {
			err := recover()

			if err != nil {
				fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS MSG :", subject, " : ", err)
			}
		}()
	}

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

		for {
			select {
			case <-signals:
				// Handle termination signals.
				fmt.Println("Received termination signal. Shutting down consumer.")
				return
			default:
				// Poll for messages.
				ev := o.consumer.Poll(100) // Adjust the timeout as needed.

				switch e := ev.(type) {
				case *kafka.Message:
					// Handle the Kafka message.
					fmt.Printf("Received message on topic %s: %s\n", *e.TopicPartition.Topic, string(e.Value))
					eventHandler(subject, string(e.Value))
				case kafka.Error:
					// Handle Kafka errors.
					fmt.Printf("Error while consuming Kafka message: %v\n", e)
				}
			}
		}
	}()

	//fmt.Println("Closing consumer...")
	//o.consumer.Close()
}

func (o *KafkaPubSubClient) RequestSubscribe(subject string, eventHandler PubSubReqEventHandler) {
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

func (o *KafkaPubSubClient) createKafkaTopic(topic string) {
	// Create a Kafka admin client
	adminClient, err := kafka.NewAdminClient(o.configMap)
	if err != nil {
		fmt.Printf("Error creating admin client: %v\n", err)
		return
	}
	defer adminClient.Close()

	// Specify the topic configuration
	topicConfig := &kafka.TopicSpecification{
		Topic:             topic,
		NumPartitions:     1, // You can adjust the number of partitions as needed.
		ReplicationFactor: 1, // You can adjust the replication factor as needed.
	}

	ctx := context.Background()

	// Create the topic
	topics := []kafka.TopicSpecification{*topicConfig}
	_, err = adminClient.CreateTopics(ctx, topics, kafka.SetAdminOperationTimeout(5000))
	if err != nil {
		fmt.Printf("Error creating topic: %v\n", err)
	} else {
		fmt.Println("Topic created successfully!")
	}
}

func (o *KafkaPubSubClient) checkTopic(topic string) {
	adminClient, err := kafka.NewAdminClient(o.configMap)
	if err != nil {
		fmt.Printf("Error creating admin client: %v\n", err)
	}
	defer adminClient.Close()

	// List all existing topics
	topics, err := adminClient.GetMetadata(nil, true, 5000)
	if err != nil {
		fmt.Printf("Error getting topic metadata: %v\n", err)
	}

	// Check if the topic exists
	if _, exists := topics.Topics[topic]; exists {
		fmt.Printf("Topic '%s' exists.\n", topic)
	} else {
		fmt.Printf("Topic '%s' does not exist.\n", topic)
		o.createKafkaTopic(topic)
	}
}

func init() {
	RegPubSubCreator("kafka", func(connString string) (PubSubClient, error) {
		ret := &KafkaPubSubClient{}

		var err error
		ret.configMap = &kafka.ConfigMap{"bootstrap.servers": connString}

		ret.producer, err = kafka.NewProducer(ret.configMap)
		if err != nil {
			return nil, err
		}

		//// for consumer group id should dynamic by aws configMap
		configConsumer := kafka.ConfigMap{
			"bootstrap.servers": connString,
			"group.id":          "my-consumer-group",
			"auto.offset.reset": "earliest",
		}

		// Create a Kafka consumer instance
		ret.consumer, err = kafka.NewConsumer(&configConsumer)
		if err != nil {
			fmt.Printf("Error creating Kafka consumer: %v\n", err)
			return nil, err
		}

		return ret, nil
	})
}
