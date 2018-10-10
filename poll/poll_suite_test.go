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
	"github.com/TerrexTech/go-kafkautils/consumer"
	"github.com/TerrexTech/go-kafkautils/producer"
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
		AggregateID:   2,
		CorrelationID: cid,
		Data:          []byte("test-data"),
		Timestamp:     time.Now(),
		UserUUID:      userUUID,
		UUID:          eventUUID,
		Version:       0,
		YearBucket:    2018,
	}

	// Produce event on Kafka topic
	testEventMsg, err := json.Marshal(mockEvent)
	Expect(err).ToNot(HaveOccurred())

	input <- producer.CreateMessage(topic, testEventMsg)
	log.Printf("====> Produced mock %s-event: %s", action, cid.String())
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
	ioConfig.ReadConfig.EnableInvalid = false

	switch channel {
	case "delete":
		ioConfig.ReadConfig.EnableDelete = true
	case "insert":
		ioConfig.ReadConfig.EnableInsert = true
	case "update":
		ioConfig.ReadConfig.EnableUpdate = true
	case "query":
		ioConfig.ReadConfig.EnableQuery = true
	case "invalid":
		ioConfig.ReadConfig.EnableInvalid = true
	}

	eventsIO, err := Init(ioConfig)
	Expect(err).ToNot(HaveOccurred())

	insertEvent := mockEvent(eventProdInput, eventsTopic, "insert")
	updateEvent := mockEvent(eventProdInput, eventsTopic, "update")
	deleteEvent := mockEvent(eventProdInput, eventsTopic, "delete")
	invalidEvent := mockEvent(eventProdInput, eventsTopic, "invalid")
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
			if e.CorrelationID.String() == insertEvent.CorrelationID.String() {
				log.Println("A matching Event appeared on insert channel")
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
			if e.CorrelationID.String() == updateEvent.CorrelationID.String() {
				log.Println("A matching Event appeared on update channel")
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
			if e.CorrelationID.String() == deleteEvent.CorrelationID.String() {
				log.Println("A matching Event appeared on delete channel")
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
			if e.CorrelationID.String() == queryEvent.CorrelationID.String() {
				log.Println("A matching Event appeared on query channel")
				if channel == "query" {
					channelSuccess = true
					return
				}
				noExtraReads = false
			}
		}
	}()

	go func() {
		for eventResp := range eventsIO.Invalid() {
			e := eventResp.Event
			Expect(eventResp.Error).ToNot(HaveOccurred())

			log.Println("An Event appeared on invalid channel")
			if e.CorrelationID.String() == invalidEvent.CorrelationID.String() {
				log.Println("A matching Event appeared on invalid channel")
				if channel == "invalid" {
					channelSuccess = true
					return
				}
				noExtraReads = false
			}
		}
	}()

	// Allow additional time for Kafka setups and warmups
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		deleteEvent    *model.Event
		insertEvent    *model.Event
		updateEvent    *model.Event
		invalidEvent   *model.Event

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
		kafkaBrokers := commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		consumerEventGroup := os.Getenv("KAFKA_CONSUMER_EVENT_GROUP")
		consumerEventQueryGroup := os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_GROUP")
		consumerEventTopic := os.Getenv("KAFKA_CONSUMER_EVENT_TOPIC")
		consumerEventQueryTopic := os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_TOPIC")
		producerEventQueryTopic := os.Getenv("KAFKA_PRODUCER_EVENT_QUERY_TOPIC")
		producerResponseTopic := os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		kc := KafkaConfig{
			Brokers:                 *kafkaBrokers,
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

	Specify("Mock events are produced", func() {
		kafkaBrokers = []string{"kafka:9092"} // commonutil.ParseHosts(os.Getenv("KAFKA_BROKERS"))
		eventsTopic = "event.rns_eventstore.events"

		prodConfig := &producer.Config{
			KafkaBrokers: kafkaBrokers,
		}
		log.Println("Creating Kafka mock-event Producer")
		p, err := producer.New(prodConfig)
		eventProdInput, err = p.Input()
		Expect(err).ToNot(HaveOccurred())

		insertEvent = mockEvent(eventProdInput, eventsTopic, "insert")
		updateEvent = mockEvent(eventProdInput, eventsTopic, "update")
		deleteEvent = mockEvent(eventProdInput, eventsTopic, "delete")
		invalidEvent = mockEvent(eventProdInput, eventsTopic, "invalid")
	})

	Context("Events are produced", func() {
		Specify("Events should appear on their respective channel", func() {
			log.Println(
				"Checking if the event-channels received the event, " +
					"with timeout of 20 seconds",
			)

			ioConfig.ReadConfig = ReadConfig{
				EnableDelete:  true,
				EnableInsert:  true,
				EnableUpdate:  true,
				EnableInvalid: true,
				EnableQuery:   false,
			}

			eventsIO, err := Init(ioConfig)
			Expect(err).ToNot(HaveOccurred())

			insertSuccess := false
			updateSuccess := false
			deleteSuccess := false
			invalidSuccess := false

			go func() {
				for eventResp := range eventsIO.Insert() {
					e := eventResp.Event
					Expect(eventResp.Error).ToNot(HaveOccurred())

					log.Println("An Event appeared on insert channel")
					if e.CorrelationID.String() == insertEvent.CorrelationID.String() {
						log.Println("A matching Event appeared on insert channel")
						insertSuccess = true
					}
				}
			}()

			go func() {
				for eventResp := range eventsIO.Update() {
					e := eventResp.Event
					Expect(eventResp.Error).ToNot(HaveOccurred())

					log.Println("An Event appeared on update channel")
					if e.CorrelationID.String() == updateEvent.CorrelationID.String() {
						log.Println("A matching Event appeared on update channel")
						updateSuccess = true
					}
				}
			}()

			go func() {
				for eventResp := range eventsIO.Delete() {
					e := eventResp.Event
					Expect(eventResp.Error).ToNot(HaveOccurred())

					log.Println("An Event appeared on delete channel")
					if e.CorrelationID.String() == deleteEvent.CorrelationID.String() {
						log.Println("A matching Event appeared on delete channel")
						deleteSuccess = true
					}
				}
			}()

			go func() {
				for eventResp := range eventsIO.Invalid() {
					e := eventResp.Event
					Expect(eventResp.Error).ToNot(HaveOccurred())

					log.Println("An Event appeared on invalid channel")
					if e.CorrelationID.String() == invalidEvent.CorrelationID.String() {
						log.Println("A matching Event appeared on invalid channel")
						invalidSuccess = true
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
					if insertSuccess && deleteSuccess && updateSuccess && invalidSuccess {
						break resultTimeoutLoop
					}
				}
			}

			defer eventsIO.Close()
			Expect(insertSuccess).To(BeTrue())
			Expect(updateSuccess).To(BeTrue())
			Expect(deleteSuccess).To(BeTrue())
			Expect(invalidSuccess).To(BeTrue())
		})
	})

	Describe("test individual channels", func() {
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

		Specify("test invalid channel", func() {
			test := channelTest(eventProdInput, eventsTopic, ioConfig, "invalid")
			Expect(test).To(BeTrue())
		})
	})

	Context("test response generation", func() {
		var (
			eventsIO     *EventsIO
			respConsumer *consumer.Consumer
		)

		BeforeEach(func() {
			var err error

			kfConfig := ioConfig.KafkaConfig
			consCfg := &consumer.Config{
				ConsumerGroup: "poll-test-group",
				KafkaBrokers:  kafkaBrokers,
				Topics:        []string{kfConfig.ProducerResponseTopic},
			}
			respConsumer, err = consumer.New(consCfg)
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
			ioConfig.ReadConfig.EnableInvalid = false

			eventsIO, err = Init(ioConfig)
			Expect(err).ToNot(HaveOccurred())

			insertEvent = mockEvent(eventProdInput, eventsTopic, "insert")
			updateEvent = mockEvent(eventProdInput, eventsTopic, "update")
			deleteEvent = mockEvent(eventProdInput, eventsTopic, "delete")
			invalidEvent = mockEvent(eventProdInput, eventsTopic, "invalid")
		})

		It("should produce the response", func(done Done) {
			eventResp := <-eventsIO.Insert()
			event := eventResp.Event
			kr := &model.KafkaResponse{
				AggregateID:   event.AggregateID,
				CorrelationID: event.CorrelationID,
			}
			eventsIO.ProduceResult() <- kr

			for msg := range respConsumer.Messages() {
				log.Println("A Response was received on response channel")
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.CorrelationID.String() == event.CorrelationID.String() {
					log.Println("The response matches")
					close(done)
					break
				}
			}
		}, 10)
	})
})
