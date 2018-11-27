package poll

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPoll(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../test.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_EVENT_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",
		"MONGO_DATABASE",
		"MONGO_CONNECTION_TIMEOUT_MS",
		"MONGO_RESOURCE_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "Poll Suite")
}

// Handler for Consumer Messages
type msgHandler struct {
	msgCallback func(*sarama.ConsumerMessage) bool
}

func (*msgHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Initializing Kafka MsgHandler")
	return nil
}

func (*msgHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Closing Kafka MsgHandler")
	return nil
}

func (m *msgHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	if m.msgCallback == nil {
		return errors.New("msgCallback cannot be nil")
	}
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}
		session.MarkMessage(msg, "")

		val := m.msgCallback(msg)
		if val {
			return nil
		}
	}
	return errors.New("required value not found")
}

type item struct {
	ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Word       string            `bson:"word,omitempty" json:"word,omitempty"`
	Definition string            `bson:"definition,omitempty" json:"definition,omitempty"`
	Hits       int               `bson:"hits,omitempty" json:"hits,omitempty"`
}

func mockEvent(
	input chan<- *sarama.ProducerMessage,
	topic string,
	action string,
) *model.Event {
	eventUUID, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	userUUID, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	mockEvent := &model.Event{
		Action:        action,
		AggregateID:   113,
		CorrelationID: cid,
		Data:          []byte("test-data"),
		NanoTime:      time.Now().UnixNano(),
		UserUUID:      userUUID,
		UUID:          eventUUID,
		Version:       233,
		YearBucket:    2018,
	}

	// Produce event on Kafka topic
	testEventMsg, err := json.Marshal(mockEvent)
	Expect(err).ToNot(HaveOccurred())

	input <- kafka.CreateMessage(topic, testEventMsg)
	log.Printf("====> Produced mock %s-event: %s on topic: %s", action, eventUUID, topic)
	return mockEvent
}
