package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"strconv"
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

	return err
}

func (o *KafkaPubSubClient) Request(subject string, msg interface{}, timeOut ...time.Duration) (string, error) {
	return "", nil
}

func (o *KafkaPubSubClient) Subscribe(subject string, eventHandler PubSubEventHandler) {
	// Create a Kafka consumer instance
	if !o.topicConsumeList[subject] {
		o.createKafkaTopic(subject) // check if kafka start before producer
		ctx, _ := context.WithCancel(context.Background())

		go func() {
			for {
				select {
				case err := <-o.configConsumer.Errors():
					log.Println("Error:", err)
				case <-ctx.Done():
					return
				default:
					log.Println("lagi:")
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
		brokers := []string{}
		delimiter3 := ","
		substrings3 := strings.Split(o.connStringList["bootstrap.servers"], delimiter3)
		for _, substring3 := range substrings3 {
			brokers = append(brokers, substring3)
		}
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
	// Create a Sarama configuration
	// config := sarama.NewConfig()
	o.configMap.Version = sarama.V2_8_0_0 // Specify the Kafka protocol version

	// Create a Kafka Admin client
	delimiter3 := ","
	brokers := []string{} // Replace with your Kafka broker addresses
	substrings3 := strings.Split(o.connStringList["bootstrap.servers"], delimiter3)
	for _, substring3 := range substrings3 {
		brokers = append(brokers, substring3)
	}
	adminClient, err := sarama.NewClusterAdmin(brokers, o.configMap)
	if err != nil {
		log.Fatalf("Error creating Kafka Admin client: %v", err)
	}

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
	replicate := "2"
	if o.connStringList["replicas"] != "" {
		replicate = o.connStringList["replicas"]
	}
	replicateInt, _ := strconv.Atoi(replicate)
	replicasPointer := &replicate
	log.Printf("replicasPointer %v", replicasPointer)
	topicConfig := &sarama.TopicDetail{
		NumPartitions:     10,                  // Number of partitions for the topic
		ReplicationFactor: int16(replicateInt), // Replication factor for the topic
		ConfigEntries:     nil,
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

}

func (o *KafkaPubSubClient) subscribeTopic(topic string, eventHandler PubSubEventHandler) {
	ctx, _ := context.WithCancel(context.Background())

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
	fmt.Printf("masuk consumer claim")
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
		delimiter3 := ","
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
		brokers := []string{} // Replace with your Kafka broker addresses
		substrings3 := strings.Split(ret.connStringList["bootstrap.servers"], delimiter3)
		for _, substring3 := range substrings3 {
			fmt.Println("server is " + substring3)
			brokers = append(brokers, substring3)
		}

		fmt.Printf("servers is %v ", brokers)

		config := sarama.NewConfig()
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Retry.Max = 5
		config.Producer.Retry.Backoff = 1000 * time.Millisecond
		// config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms
		config.Producer.Return.Successes = true
		config.Admin.Timeout = 5000 * time.Millisecond

		ret.producer, err = sarama.NewSyncProducer(brokers, config)
		if err != nil {
			// log.Fatalf("Failed to start Sarama producer: %v", err)
			fmt.Printf("Failed to start Sarama producer: %v\n", err)
			return nil, err
		}

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
