package poll

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
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
		"KAFKA_PRODUCER_RESPONSE_TOPIC",

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
	eventUUID, err := uuuid.NewV1()
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
		Timestamp:     time.Now(),
		UserUUID:      userUUID,
		TimeUUID:      eventUUID,
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

func channelTest(
	eventProdInput chan<- *sarama.ProducerMessage,
	eventsTopic string,
	ioConfig IOConfig,
	channel string,
) bool {
	ioConfig.ReadConfig = ReadConfig{
		EnableDelete: false,
		EnableInsert: false,
		EnableUpdate: false,
		EnableQuery:  false,
	}

	switch channel {
	case "delete":
		ioConfig.ReadConfig.EnableDelete = true
	case "insert":
		ioConfig.ReadConfig.EnableInsert = true
	case "update":
		ioConfig.ReadConfig.EnableUpdate = true
	case "query":
		ioConfig.ReadConfig.EnableQuery = true
	}

	eventsIO, err := Init(ioConfig)
	Expect(err).ToNot(HaveOccurred())

	insertEvent := mockEvent(eventProdInput, eventsTopic, "insert")
	updateEvent := mockEvent(eventProdInput, eventsTopic, "update")
	deleteEvent := mockEvent(eventProdInput, eventsTopic, "delete")
	queryEvent := mockEvent(eventProdInput, eventsTopic, "query")

	log.Printf(
		"Checking if the %s-channel received the event, "+
			"with timeout of 10 seconds",
		channel,
	)

	channelSuccess := false
	var csLock sync.RWMutex

	noExtraReads := true
	var erLock sync.RWMutex

	g := eventsIO.RoutinesGroup()

	// Allow additional time for Kafka setups and warmups
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.New("timed out")
			case eventResp := <-eventsIO.Insert():
				if eventResp == nil {
					continue
				}
				e := eventResp.Event
				Expect(eventResp.Error).ToNot(HaveOccurred())

				log.Println("An Event appeared on insert channel")
				cidMatch := e.CorrelationID == insertEvent.CorrelationID
				uuidMatch := e.TimeUUID == insertEvent.TimeUUID
				if uuidMatch && cidMatch {
					log.Println("==> A matching Event appeared on insert channel ")
					if channel == "insert" {
						csLock.Lock()
						channelSuccess = true
						csLock.Unlock()
						return nil
					}
					erLock.Lock()
					noExtraReads = false
					erLock.Unlock()
				}
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.New("timed out")
			case eventResp := <-eventsIO.Update():
				if eventResp == nil {
					continue
				}
				e := eventResp.Event
				Expect(eventResp.Error).ToNot(HaveOccurred())

				log.Println("An Event appeared on update channel")
				cidMatch := e.CorrelationID == updateEvent.CorrelationID
				uuidMatch := e.TimeUUID == updateEvent.TimeUUID
				if uuidMatch && cidMatch {
					log.Println("==> A matching Event appeared on update channel")
					if channel == "update" {
						csLock.Lock()
						channelSuccess = true
						csLock.Unlock()
						return nil
					}
					erLock.Lock()
					noExtraReads = false
					erLock.Unlock()
				}
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.New("timed out")
			case eventResp := <-eventsIO.Delete():
				if eventResp == nil {
					continue
				}
				e := eventResp.Event
				Expect(eventResp.Error).ToNot(HaveOccurred())

				log.Println("An Event appeared on delete channel")
				cidMatch := e.CorrelationID == deleteEvent.CorrelationID
				uuidMatch := e.TimeUUID == deleteEvent.TimeUUID
				if uuidMatch && cidMatch {
					log.Println("==> A matching Event appeared on delete channel")
					if channel == "delete" {
						csLock.Lock()
						channelSuccess = true
						csLock.Unlock()
						return nil
					}
					erLock.Lock()
					noExtraReads = false
					erLock.Unlock()
				}
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.New("timed out")
			case eventResp := <-eventsIO.Query():
				if eventResp == nil {
					continue
				}
				e := eventResp.Event
				Expect(eventResp.Error).ToNot(HaveOccurred())

				log.Println("An Event appeared on query channel")
				cidMatch := e.CorrelationID == queryEvent.CorrelationID
				uuidMatch := e.TimeUUID == queryEvent.TimeUUID
				if uuidMatch && cidMatch {
					log.Println("==> A matching Event appeared on query channel")
					if channel == "query" {
						csLock.Lock()
						channelSuccess = true
						csLock.Unlock()
						return nil
					}
					erLock.Lock()
					noExtraReads = false
					erLock.Unlock()
				}
			}
		}
	})

resultTimeoutLoop:
	for {
		select {
		case <-ctx.Done():
			break resultTimeoutLoop
		default:
			erLock.RLock()
			er := noExtraReads
			erLock.RUnlock()
			if !er {
				break resultTimeoutLoop
			}
		}
	}

	eventsIO.Close()
	<-eventsIO.Wait()

	cs := channelSuccess
	er := noExtraReads

	log.Println("===> Channel: "+channel, eventsTopic)
	log.Printf("ChannelSuccess: %t", cs)
	log.Printf("NoExtraReads: %t", er)
	return cs && er
}
