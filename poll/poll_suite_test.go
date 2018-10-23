package poll

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
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

		"MONGO_TEST_HOSTS",
		"MONGO_TEST_USERNAME",
		"MONGO_TEST_PASSWORD",
		"MONGO_TEST_DATABASE",
		"MONGO_TEST_CONNECTION_TIMEOUT_MS",
		"MONGO_TEST_RESOURCE_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required, but is not set", missingVar)
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
		AggregateID:   2,
		CorrelationID: cid,
		Data:          []byte("test-data"),
		Timestamp:     time.Now(),
		UserUUID:      userUUID,
		TimeUUID:      eventUUID,
		Version:       0,
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
	ioConfig.ReadConfig.EnableDelete = false
	ioConfig.ReadConfig.EnableInsert = false
	ioConfig.ReadConfig.EnableUpdate = false
	ioConfig.ReadConfig.EnableQuery = false

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
	noExtraReads := true

	go func() {
		for eventResp := range eventsIO.Insert() {
			e := eventResp.Event
			Expect(eventResp.Error).ToNot(HaveOccurred())

			log.Println("An Event appeared on insert channel")
			if e.CorrelationID == insertEvent.CorrelationID {
				log.Println("==> A matching Event appeared on insert channel")
				if channel == "insert" {
					channelSuccess = true
					return
				}
				noExtraReads = false
			}
		}
	}()

	go func() {
		for eventResp := range eventsIO.Update() {
			e := eventResp.Event
			Expect(eventResp.Error).ToNot(HaveOccurred())

			log.Println("An Event appeared on update channel")
			if e.CorrelationID == updateEvent.CorrelationID {
				log.Println("==> A matching Event appeared on update channel")
				if channel == "update" {
					channelSuccess = true
					return
				}
				noExtraReads = false
			}
		}
	}()

	go func() {
		for eventResp := range eventsIO.Delete() {
			e := eventResp.Event
			Expect(eventResp.Error).ToNot(HaveOccurred())

			log.Println("An Event appeared on delete channel")
			log.Println(e.CorrelationID)
			log.Println(deleteEvent.CorrelationID)
			if e.CorrelationID == deleteEvent.CorrelationID {
				log.Println("==> A matching Event appeared on delete channel")
				if channel == "delete" {
					channelSuccess = true
					return
				}
				noExtraReads = false
			}
		}
	}()

	go func() {
		for eventResp := range eventsIO.Query() {
			e := eventResp.Event
			Expect(eventResp.Error).ToNot(HaveOccurred())

			log.Println("An Event appeared on query channel")
			if e.CorrelationID == queryEvent.CorrelationID {
				log.Println("==> A matching Event appeared on query channel")
				if channel == "query" {
					channelSuccess = true
					return
				}
				noExtraReads = false
			}
		}
	}()

	// Allow additional time for Kafka setups and warmups
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

resultTimeoutLoop:
	for {
		select {
		case <-ctx.Done():
			break resultTimeoutLoop
		default:
			if !noExtraReads {
				break resultTimeoutLoop
			}
		}
	}

	eventsIO.Close()
	log.Println("===> Channel: "+channel, eventsTopic)
	log.Printf("ChannelSuccess: %t", channelSuccess)
	log.Printf("NoExtraReads: %t", noExtraReads)
	return channelSuccess && noExtraReads
}

var _ = Describe("Poll Suite", func() {
	var (
		kafkaBrokers []string

		eventsTopic    string
		eventProdInput chan<- *sarama.ProducerMessage

		ioConfig IOConfig
	)

	BeforeSuite(func() {
		// ==========> Mongo Setup
		mongoHosts := commonutil.ParseHosts(
			os.Getenv("MONGO_TEST_HOSTS"),
		)
		mongoUsername := os.Getenv("MONGO_TEST_USERNAME")
		mongoPassword := os.Getenv("MONGO_TEST_PASSWORD")

		mongoConnTimeoutStr := os.Getenv("MONGO_TEST_CONNECTION_TIMEOUT_MS")
		mongoConnTimeout, err := strconv.Atoi(mongoConnTimeoutStr)
		if err != nil {
			err = errors.Wrap(err, "Error converting MONGO_TEST_CONNECTION_TIMEOUT_MS to integer")
			log.Println(err)
			log.Println("A defalt value of 3000 will be used for MONGO_TEST_CONNECTION_TIMEOUT_MS")
			mongoConnTimeout = 3000
		}

		mongoResTimeoutStr := os.Getenv("MONGO_TEST_CONNECTION_TIMEOUT_MS")
		mongoResTimeout, err := strconv.Atoi(mongoResTimeoutStr)
		if err != nil {
			err = errors.Wrap(err, "Error converting MONGO_TEST_RESOURCE_TIMEOUT_MS to integer")
			log.Println(err)
			log.Println("A defalt value of 5000 will be used for MONGO_TEST_RESOURCE_TIMEOUT_MS")
			mongoConnTimeout = 5000
		}

		mongoDatabase := os.Getenv("MONGO_TEST_DATABASE")

		mongoFailThresholdStr := os.Getenv("MONGO_TEST_FAIL_THRESHOLD")
		mongoFailThreshold, err := strconv.Atoi(mongoFailThresholdStr)
		if err != nil {
			err = errors.Wrap(err, "Error converting MONGO_TEST_FAIL_THRESHOLD to integer")
			log.Println(err)
			log.Println("A defalt value of 120 will be used for MONGO_TEST_FAIL_THRESHOLD")
			mongoFailThreshold = 120
		}

		mongoConfig := mongo.ClientConfig{
			Hosts:               *mongoHosts,
			Username:            mongoUsername,
			Password:            mongoPassword,
			TimeoutMilliseconds: uint32(mongoConnTimeout),
		}

		// ====> MongoDB Client
		client, err := mongo.NewClient(mongoConfig)
		Expect(err).ToNot(HaveOccurred())

		conn := &mongo.ConnectionConfig{
			Client:  client,
			Timeout: uint32(mongoResTimeout),
		}
		// Index Configuration
		indexConfigs := []mongo.IndexConfig{
			mongo.IndexConfig{
				ColumnConfig: []mongo.IndexColumnConfig{
					mongo.IndexColumnConfig{
						Name:        "word",
						IsDescOrder: true,
					},
				},
				IsUnique: true,
				Name:     "test_index",
			},
		}

		// ====> Create New Collection
		c := &mongo.Collection{
			Connection:   conn,
			Name:         "test_coll",
			Database:     mongoDatabase,
			SchemaStruct: &item{},
			Indexes:      indexConfigs,
		}
		collection, err := mongo.EnsureCollection(c)
		Expect(err).ToNot(HaveOccurred())

		// ==========> Kafka Setup
		kafkaBrokers = *commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		eventsTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")

		consumerEventGroup := os.Getenv("KAFKA_CONSUMER_EVENT_GROUP")
		consumerEventQueryGroup := os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_GROUP")
		consumerEventTopic := os.Getenv("KAFKA_CONSUMER_EVENT_TOPIC")
		consumerEventQueryTopic := os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_TOPIC")
		producerEventQueryTopic := os.Getenv("KAFKA_PRODUCER_EVENT_QUERY_TOPIC")
		producerResponseTopic := os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		kc := KafkaConfig{
			Brokers:                 kafkaBrokers,
			ConsumerEventGroup:      consumerEventGroup,
			ConsumerEventQueryGroup: consumerEventQueryGroup,
			ConsumerEventTopic:      consumerEventTopic,
			ConsumerEventQueryTopic: consumerEventQueryTopic,
			ProducerEventQueryTopic: producerEventQueryTopic,
			ProducerResponseTopic:   producerResponseTopic,
		}
		ioConfig = IOConfig{
			AggregateID:        2,
			ReadConfig:         ReadConfig{},
			KafkaConfig:        kc,
			MongoCollection:    collection,
			MongoFailThreshold: int16(mongoFailThreshold),
		}

		prodConfig := &kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		}
		log.Println("Creating Kafka mock-event Producer")
		p, err := kafka.NewProducer(prodConfig)
		Expect(err).ToNot(HaveOccurred())
		eventProdInput = p.Input()
	})

	It("should return error if AggregateID is not specified", func() {
		aid := ioConfig.AggregateID

		ioConfig.AggregateID = 0
		eventsIO, err := Init(ioConfig)
		Expect(err).To(HaveOccurred())
		Expect(eventsIO).To(BeNil())

		ioConfig.AggregateID = -2
		eventsIO, err = Init(ioConfig)
		Expect(err).To(HaveOccurred())
		Expect(eventsIO).To(BeNil())

		ioConfig.AggregateID = aid
	})

	It("should return error if MongoFailThreshold is not specified", func() {
		mft := ioConfig.MongoFailThreshold

		ioConfig.MongoFailThreshold = 0
		eventsIO, err := Init(ioConfig)
		Expect(err).To(HaveOccurred())
		Expect(eventsIO).To(BeNil())

		ioConfig.MongoFailThreshold = -2
		eventsIO, err = Init(ioConfig)
		Expect(err).To(HaveOccurred())
		Expect(eventsIO).To(BeNil())

		ioConfig.MongoFailThreshold = mft
	})

	Describe("KafkaConfig Validation", func() {
		It("should return error if ConsumerEventGroup is not specified", func() {
			kfConfig := ioConfig.KafkaConfig
			group := kfConfig.ConsumerEventGroup

			kfConfig.ConsumerEventGroup = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())

			kfConfig.ConsumerEventGroup = group
		})

		It("should return error if ConsumerEventQueryTopic is not specified", func() {
			kfConfig := ioConfig.KafkaConfig
			group := kfConfig.ConsumerEventQueryTopic

			kfConfig.ConsumerEventQueryTopic = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())

			kfConfig.ConsumerEventQueryTopic = group
		})

		It("should return error if ConsumerEventQueryTopic is not specified", func() {
			kfConfig := ioConfig.KafkaConfig
			topic := kfConfig.ConsumerEventQueryTopic

			kfConfig.ConsumerEventQueryTopic = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())

			kfConfig.ConsumerEventQueryTopic = topic
		})

		It("should return error if ConsumerEventTopic is not specified", func() {
			kfConfig := ioConfig.KafkaConfig
			topic := kfConfig.ConsumerEventTopic

			kfConfig.ConsumerEventTopic = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())

			kfConfig.ConsumerEventTopic = topic
		})

		It("should return error if ProducerEventQueryTopic is not specified", func() {
			kfConfig := ioConfig.KafkaConfig
			topic := kfConfig.ProducerEventQueryTopic

			kfConfig.ProducerEventQueryTopic = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())

			kfConfig.ProducerEventQueryTopic = topic
		})

		It("should return error if ProducerResponseTopic is not specified", func() {
			kfConfig := ioConfig.KafkaConfig
			topic := kfConfig.ProducerResponseTopic

			kfConfig.ProducerResponseTopic = ""
			eventsIO, err := Init(ioConfig)
			Expect(err).To(HaveOccurred())
			Expect(eventsIO).To(BeNil())

			kfConfig.ProducerResponseTopic = topic
		})
	})

	It("should return error on invalid ReadConfig", func() {
		rc := ioConfig.ReadConfig
		// bool zero-value is false
		ioConfig.ReadConfig = ReadConfig{}

		eventsIO, err := Init(ioConfig)
		Expect(err).To(HaveOccurred())
		Expect(eventsIO).To(BeNil())

		ioConfig.ReadConfig = rc
	})

	// Context("Events are produced", func() {
	Specify("Events should appear on their respective channel", func() {
		insertEvent := mockEvent(eventProdInput, eventsTopic, "insert")
		updateEvent := mockEvent(eventProdInput, eventsTopic, "update")
		deleteEvent := mockEvent(eventProdInput, eventsTopic, "delete")

		log.Println(
			"Checking if the event-channels received the event, " +
				"with timeout of 20 seconds",
		)

		ioConfig.ReadConfig = ReadConfig{
			EnableDelete: true,
			EnableInsert: true,
			EnableUpdate: true,
			EnableQuery:  false,
		}

		eventsIO, err := Init(ioConfig)
		Expect(err).ToNot(HaveOccurred())

		insertSuccess := false
		updateSuccess := false
		deleteSuccess := false

		closeChan := make(chan struct{})
		go func() {
			for {
				select {
				case <-closeChan:
					return
				case eventResp := <-eventsIO.Insert():
					e := eventResp.Event
					Expect(eventResp.Error).ToNot(HaveOccurred())

					log.Println("An Event appeared on insert channel")
					if e.CorrelationID == insertEvent.CorrelationID {
						log.Println("A matching Event appeared on insert channel")
						insertSuccess = true
						return
					}
				}
			}
		}()

		go func() {
			for {
				select {
				case <-closeChan:
					return
				case eventResp := <-eventsIO.Update():
					e := eventResp.Event
					Expect(eventResp.Error).ToNot(HaveOccurred())

					log.Println("An Event appeared on update channel")
					if e.CorrelationID == updateEvent.CorrelationID {
						log.Println("A matching Event appeared on update channel")
						updateSuccess = true
						return
					}
				}
			}
		}()

		go func() {
			for {
				select {
				case <-closeChan:
					return
				case eventResp := <-eventsIO.Delete():
					e := eventResp.Event
					Expect(eventResp.Error).ToNot(HaveOccurred())

					log.Println("An Event appeared on delete channel")
					if e.CorrelationID == deleteEvent.CorrelationID {
						log.Println("A matching Event appeared on delete channel")
						deleteSuccess = true
						return
					}
				}
			}
		}()

		// Allow additional time for Kafka setups and warmups
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

	resultTimeoutLoop:
		for {
			select {
			case <-ctx.Done():
				closeChan <- struct{}{}
				break resultTimeoutLoop
			default:
				if insertSuccess && deleteSuccess && updateSuccess {
					break resultTimeoutLoop
				}
			}
		}

		eventsIO.Close()
		Expect(insertSuccess).To(BeTrue())
		Expect(updateSuccess).To(BeTrue())
		Expect(deleteSuccess).To(BeTrue())
	})

	Specify("test delete channel", func() {
		test := channelTest(eventProdInput, eventsTopic, ioConfig, "delete")
		Expect(test).To(BeTrue())
	})

	Specify("test insert channel", func() {
		test := channelTest(eventProdInput, eventsTopic, ioConfig, "insert")
		Expect(test).To(BeTrue())
	})

	Specify("test update channel", func() {
		test := channelTest(eventProdInput, eventsTopic, ioConfig, "update")
		Expect(test).To(BeTrue())
	})

	Specify("test query channel", func() {
		test := channelTest(eventProdInput, eventsTopic, ioConfig, "query")
		Expect(test).To(BeTrue())
	})

	Context("test response generation", func() {
		var (
			eventsIO     *EventsIO
			respConsumer *kafka.Consumer
		)

		BeforeEach(func() {
			var err error

			kfConfig := ioConfig.KafkaConfig
			consCfg := &kafka.ConsumerConfig{
				GroupName:    "poll-test-group",
				KafkaBrokers: kafkaBrokers,
				Topics:       []string{kfConfig.ProducerResponseTopic},
			}
			respConsumer, err = kafka.NewConsumer(consCfg)
			Expect(err).ToNot(HaveOccurred())

			go func() {
				for e := range respConsumer.Errors() {
					Expect(e).ToNot(HaveOccurred())
				}
			}()

			ioConfig.ReadConfig.EnableDelete = false
			ioConfig.ReadConfig.EnableInsert = true
			ioConfig.ReadConfig.EnableUpdate = false
			ioConfig.ReadConfig.EnableQuery = false

			eventsIO, err = Init(ioConfig)
			Expect(err).ToNot(HaveOccurred())

			// Here we again test that we only get responses on channels we have enabled
			mockEvent(eventProdInput, eventsTopic, "insert")
			mockEvent(eventProdInput, eventsTopic, "update")
			mockEvent(eventProdInput, eventsTopic, "delete")
		})

		It("should produce the response", func(done Done) {
			// Since we only enabled insert
			eventResp := <-eventsIO.Insert()
			event := eventResp.Event
			kr := &model.KafkaResponse{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
				UUID:          event.TimeUUID,
			}
			eventsIO.ProduceResult() <- kr
			eventsIO.Close()

			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				log.Println("A Response was received on response channel")
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				cidMatch := kr.CorrelationID == event.CorrelationID
				uuidMatch := kr.UUID == event.TimeUUID
				if uuidMatch && cidMatch {
					log.Println("The response matches")
					close(done)
					return true
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			respConsumer.Consume(ctx, handler)
		}, 15)
	})
})
