package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"strings"
	"time"
)

type KafkaPubSubClient struct {
	producer         sarama.SyncProducer
	configMap        *sarama.Config
	configConsumer   sarama.ConsumerGroup
	topicList        map[string]bool
	topicConsumeList map[string]bool
	topicQueueList   map[string]bool
	connStringList   map[string]string
	connString       string
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
	topic := subject

	// check the topic is existed or not // if not exist will create new topic
	// o.checkTopic(topic)
	log.Printf("msgByte is %s", msgByte)
	msgKafka := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msgByte),
	}

	partition, offset, err := o.producer.SendMessage(msgKafka)
	if err != nil {
		log.Printf("Failed to produce message: %v", err)
	} else {
		log.Printf("Produced message to topic %s, partition %d, offset %d\n", topic, partition, offset)
	}

	//defer func() {
	//	if errProd := ret.producer.Close(); errProd != nil {
	//		log.Fatalln("Failed to close producer:", errProd)
	//	}
	//}()

	return err
}

func (o *KafkaPubSubClient) Request(subject string, msg interface{}, timeOut ...time.Duration) (string, error) {
	return "", nil
}

func (o *KafkaPubSubClient) Subscribe(subject string, eventHandler PubSubEventHandler) {
	// Create a Kafka consumer instance
	if !o.topicConsumeList[subject] {
		o.createKafkaTopic(subject) // check if kafka start before producer
		// fmt.Printf("New Consumer '%s' created.\n", subject)
		// go o.consumeTopic(subject, eventHandler)

		ctx, _ := context.WithCancel(context.Background())
		//defer cancel()

		go func() {
			for {
				select {
				case err := <-o.configConsumer.Errors():
					log.Println("Error:", err)
				case <-ctx.Done():
					return
				default:
					if err := o.configConsumer.Consume(ctx, []string{subject}, newConsumerHandler(eventHandler)); err != nil {
						log.Fatal(err)
					}
				}
			}
		}()

		o.topicConsumeList[subject] = true
	}
}

func (o *KafkaPubSubClient) RequestSubscribe(subject string, eventHandler PubSubReqEventHandler) {
}

func (o *KafkaPubSubClient) QueueSubscribe(subject string, queue string, eventHandler PubSubEventHandler) {
	if !o.topicQueueList[subject] {
		o.createKafkaTopic(subject) // check if kafka start before producer
		//configConsumer := &kafka.ConfigMap{
		//	"bootstrap.servers": o.connStringList["bootstrap.servers"],
		//	"security.protocol": o.connStringList["security.protocol"],
		//	"group.id":          queue,
		//}
		//
		//if o.connStringList["security.protocol"] != "PLAINTEXT" {
		//	configConsumer.SetKey("sasl.mechanism", o.connStringList["sasl.mechanism"])
		//	configConsumer.SetKey("sasl.username", o.connStringList["sasl.username"])
		//	configConsumer.SetKey("sasl.password", o.connStringList["sasl.password"])
		//}

		// c, err := kafka.NewConsumer(configConsumer)
		brokers := []string{o.connStringList["bootstrap.servers"]}
		queueConsumer, err := sarama.NewConsumerGroup(brokers, queue, o.configMap)
		if err != nil {
			fmt.Printf("Error creating queue Kafka consumer: %v\n", err)
			return
		}
		//
		//go o.subscribeTopic(c, subject, eventHandler)
		ctx, _ := context.WithCancel(context.Background())
		//defer cancel()

		go func() {
			for {
				select {
				case err := <-queueConsumer.Errors():
					log.Println("Error:", err)
				case <-ctx.Done():
					return
				default:
					if err := queueConsumer.Consume(ctx, []string{subject}, newConsumerHandler(eventHandler)); err != nil {
						log.Fatal(err)
					}
				}
			}
		}()
		o.topicQueueList[subject] = true
	}
}

func (o *KafkaPubSubClient) createKafkaTopic(topic string) {
	//adminClient, err := kafka.NewAdminClient(o.configMap)
	//if err != nil {
	//	fmt.Printf("Error creating admin client: %v\n", err)
	//	return
	//}
	//defer adminClient.Close()
	//
	//md, err := adminClient.GetMetadata(nil, true, 10000)
	//
	//if err != nil {
	//	fmt.Printf("Error get metadata : %v\n", err)
	//	return
	//}
	//
	//for name := range md.Topics {
	//
	//	if name == topic {
	//		fmt.Println("Topic already exist in server :", topic)
	//		o.topicList[topic] = true
	//		return
	//	}
	//}
	//
	//// Specify the topic configuration
	//topicConfig := &kafka.TopicSpecification{
	//	Topic:             topic,
	//	NumPartitions:     10, // You can adjust the number of partitions as needed.
	//	ReplicationFactor: 1,  // You can adjust the replication factor as needed.
	//}
	//
	//ctx := context.Background()
	//
	//// Create the topic
	//topics := []kafka.TopicSpecification{*topicConfig}
	//_, err = adminClient.CreateTopics(ctx, topics, kafka.SetAdminOperationTimeout(5000))
	//if err != nil {
	//	fmt.Printf("Error creating topic: %v\n", err)
	//} else {
	//	o.topicList[topic] = true
	//	fmt.Println("Topic created successfully!")
	//}

	// Create a Sarama configuration
	config := sarama.NewConfig()
	o.configMap.Version = sarama.V2_8_0_0 // Specify the Kafka protocol version

	// Create a Kafka Admin client
	brokerList := []string{o.connStringList["bootstrap.servers"]}
	adminClient, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		log.Fatalf("Error creating Kafka Admin client: %v", err)
	}
	defer func() {
		if err := adminClient.Close(); err != nil {
			log.Printf("Error closing Kafka Admin client: %v", err)
		}
	}()

	topics, err := adminClient.ListTopics()
	if err != nil {
		log.Fatalf("Error getting topic list: %v", err)
	}

	// Print the list of topics
	log.Println("List of Kafka topics:")
	for topicSearch := range topics {
		if topicSearch == topic {
			log.Println("topic already exists")
			return
		}
		// log.Println(topic)
	}

	// Define the topic configuration
	topicConfig := &sarama.TopicDetail{
		NumPartitions:     10,  // Number of partitions for the topic
		ReplicationFactor: 1,   // Replication factor for the topic
		ConfigEntries:     nil, // Additional topic configuration entries (can be nil)
	}

	// Specify the topic name
	topicName := topic

	// Create the Kafka topic
	err = adminClient.CreateTopic(topicName, topicConfig, false)
	if err != nil {
		log.Fatalf("Error creating Kafka topic: %v", err)
	}
	log.Printf("Kafka topic '%s' created successfully", topicName)
}

func (o *KafkaPubSubClient) checkTopic(topic string) {
	// Check if the topic exists
	if o.topicList[topic] {
		fmt.Printf("Topic '%s' exists.\n", topic)
	} else {
		fmt.Printf("Topic '%s' does not exist.\n", topic)
		o.createKafkaTopic(topic)
	}
}

func (o *KafkaPubSubClient) consumeTopic(topic string, eventHandler PubSubEventHandler) {
	//c, err := kafka.NewConsumer(o.configConsumer)
	//if err != nil {
	//	fmt.Printf("Error creating Kafka consumer: %v\n", err)
	//	return
	//}
	//
	//defer c.Close()
	//
	////o.checkTopic(topic)
	//o.subscribeTopic(c, topic, eventHandler)
}

func (o *KafkaPubSubClient) subscribeTopic(c *kafka.Consumer, topic string, eventHandler PubSubEventHandler) {
	//err := c.SubscribeTopics([]string{topic}, nil)
	//if err != nil {
	//	fmt.Printf("Error subscribing to topic: %v\n", err)
	//	defer func() {
	//		err := recover()
	//
	//		if err != nil {
	//			fmt.Println("=====> SYSTEM PANIC WHEN PROCESS NATS MSG :", topic, " : ", err)
	//		}
	//	}()
	//}
	//
	//fmt.Printf("Consumer for topic %s started\n", topic)
	//for {
	//	ev := c.Poll(100) // Adjust the timeout as needed.
	//	switch e := ev.(type) {
	//	case *kafka.Message:
	//		// Handle the Kafka message.
	//		fmt.Printf("Received message on topic %s: %s\n", *e.TopicPartition.Topic, string(e.Value))
	//		eventHandler(topic, string(e.Value))
	//	case kafka.Error:
	//		// Handle Kafka errors.
	//		fmt.Printf("Error while consuming Kafka message: %v\n", e)
	//	}
	//}

	//config := sarama.NewConfig()
	//config.Consumer.Return.Errors = true

	//brokers := []string{"<broker_ip>:9092"} // Replace with your Kafka broker address
	//
	//consumerGroup, err := sarama.NewConsumerGroup(brokers, "my-group", config)
	//if err != nil {
	//	log.Fatal(err)
	//}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case err := <-o.configConsumer.Errors():
				log.Println("Error:", err)
			case <-ctx.Done():
				return
			default:
				if err := o.configConsumer.Consume(ctx, []string{topic}, newConsumerHandler(eventHandler)); err != nil {
					log.Fatal(err)
				}
			}
		}
	}()
}

type ConsumerHandler struct {
	EventHandler PubSubEventHandler
}

func newConsumerHandler(eventHandler PubSubEventHandler) sarama.ConsumerGroupHandler {
	return &ConsumerHandler{
		EventHandler: eventHandler,
	}
}

func (h *ConsumerHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Printf("Consumed message from partition %d with offset %d: %s\n", message.Partition, message.Offset, string(message.Value))
		h.EventHandler(message.Topic, string(message.Value))
		session.MarkMessage(message, "")
	}

	return nil
}

func parseKafkaMessage(message *sarama.ConsumerMessage) (PubSubEventHandler, error) {
	// Assuming messages are JSON-encoded. Adjust the parsing logic based on your message format.
	var eventHandler PubSubEventHandler
	log.Println(message.Topic)
	log.Println(message.Value)
	// eventHandler("topic", "val")
	return eventHandler, nil
}

func init() {
	RegPubSubCreator("kafka", func(connString string) (PubSubClient, error) {
		ret := &KafkaPubSubClient{}

		var err error

		// add tagging
		inputString := connString
		delimiter := ";"
		delimiter2 := "="
		mappingString := make(map[string]string)
		ret.connStringList = mappingString

		// Use strings.Split to split the string
		substrings := strings.Split(inputString, delimiter)

		// Print the resulting substrings
		for _, substring := range substrings {
			fmt.Println(substring)
			substrings2 := strings.Split(substring, delimiter2)
			ret.connStringList[substrings2[0]] = substrings2[1]
		}

		ret.connString = ret.connStringList["bootstrap.servers"]
		//ret.configMap = &kafka.ConfigMap{
		//	"bootstrap.servers": ret.connStringList["bootstrap.servers"],
		//	"security.protocol": ret.connStringList["security.protocol"],
		//}

		//if ret.connStringList["security.protocol"] != "PLAINTEXT" {
		//	ret.configMap.SetKey("sasl.mechanism", ret.connStringList["sasl.mechanism"])
		//	ret.configMap.SetKey("sasl.username", ret.connStringList["sasl.username"])
		//	ret.configMap.SetKey("sasl.password", ret.connStringList["sasl.password"])
		//}

		//ret.producer, err = kafka.NewProducer(ret.configMap)
		//if err != nil {
		//	return nil, err
		//}

		brokers := []string{ret.connStringList["bootstrap.servers"]} // Replace with your Kafka broker addresses

		config := sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 5
		config.Producer.Retry.Backoff = 1000 * time.Millisecond
		// config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
		config.Producer.Return.Successes = true

		ret.producer, err = sarama.NewSyncProducer(brokers, config)
		if err != nil {
			// log.Fatalf("Failed to start Sarama producer: %v", err)
			fmt.Printf("Failed to start Sarama producer: %v\n", err)
			return nil, err
		}

		// ret.producer = producer

		//defer func() {
		//	if errProd := ret.producer.Close(); errProd != nil {
		//		log.Fatalln("Failed to close producer:", errProd)
		//	}
		//}()

		//// for consumer group id should dynamic by aws configMap
		//ret.configConsumer = &kafka.ConfigMap{
		//	"bootstrap.servers": ret.connStringList["bootstrap.servers"],
		//	"security.protocol": ret.connStringList["security.protocol"],
		//	"group.id":          "my-consumer-group",
		//	"auto.offset.reset": "earliest",
		//}

		//if ret.connStringList["security.protocol"] != "PLAINTEXT" {
		//	ret.configConsumer.SetKey("sasl.mechanism", ret.connStringList["sasl.mechanism"])
		//	ret.configConsumer.SetKey("sasl.username", ret.connStringList["sasl.username"])
		//	ret.configConsumer.SetKey("sasl.password", ret.connStringList["sasl.password"])
		//}

		config.Consumer.Return.Errors = true
		ret.configConsumer, err = sarama.NewConsumerGroup(brokers, "my-group", config)
		if err != nil {
			log.Fatal(err)
		}

		ret.configMap = config

		mapping := make(map[string]bool)
		if ret.topicList == nil {
			ret.topicList = mapping
		}
		if ret.topicConsumeList == nil {
			ret.topicConsumeList = mapping
		}
		if ret.topicQueueList == nil {
			ret.topicQueueList = mapping
		}

		return ret, nil
	})
}
