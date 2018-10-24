package poll

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("Consumers", func() {
	var (
		kafkaBrokers []string

		eventsTopic    string
		eventProdInput chan<- *sarama.ProducerMessage

		ioConfig IOConfig
	)

	BeforeSuite(func() {
		// ==========> Mongo Setup
		mongoHosts := commonutil.ParseHosts(
			os.Getenv("MONGO_HOSTS"),
		)
		mongoUsername := os.Getenv("MONGO_USERNAME")
		mongoPassword := os.Getenv("MONGO_PASSWORD")

		mongoConnTimeoutStr := os.Getenv("MONGO_CONNECTION_TIMEOUT_MS")
		mongoConnTimeout, err := strconv.Atoi(mongoConnTimeoutStr)
		if err != nil {
			err = errors.Wrap(err, "Error converting MONGO_CONNECTION_TIMEOUT_MS to integer")
			log.Println(err)
			log.Println("A defalt value of 3000 will be used for MONGO_CONNECTION_TIMEOUT_MS")
			mongoConnTimeout = 3000
		}

		mongoResTimeoutStr := os.Getenv("MONGO_CONNECTION_TIMEOUT_MS")
		mongoResTimeout, err := strconv.Atoi(mongoResTimeoutStr)
		if err != nil {
			err = errors.Wrap(err, "Error converting MONGO_RESOURCE_TIMEOUT_MS to integer")
			log.Println(err)
			log.Println("A defalt value of 5000 will be used for MONGO_RESOURCE_TIMEOUT_MS")
			mongoConnTimeout = 5000
		}

		mongoDatabase := os.Getenv("MONGO_DATABASE")

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
		mc := MongoConfig{
			AggregateID:        113,
			AggCollection:      collection,
			Connection:         conn,
			MetaDatabaseName:   mongoDatabase,
			MetaCollectionName: "test_meta",
		}
		ioConfig = IOConfig{
			ReadConfig:  ReadConfig{},
			KafkaConfig: kc,
			MongoConfig: mc,
		}

		prodConfig := &kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		}
		log.Println("Creating Kafka mock-event Producer")
		p, err := kafka.NewProducer(prodConfig)
		Expect(err).ToNot(HaveOccurred())
		eventProdInput = p.Input()
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
					cidMatch := e.CorrelationID == insertEvent.CorrelationID
					uuidMatch := e.TimeUUID == insertEvent.TimeUUID
					if uuidMatch && cidMatch {
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
					cidMatch := e.CorrelationID == updateEvent.CorrelationID
					uuidMatch := e.TimeUUID == updateEvent.TimeUUID
					if uuidMatch && cidMatch {
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
					cidMatch := e.CorrelationID == deleteEvent.CorrelationID
					uuidMatch := e.TimeUUID == deleteEvent.TimeUUID
					if uuidMatch && cidMatch {
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

	It("should update Aggregate-meta", func() {
		// Generate a mock-Event
		test := channelTest(eventProdInput, eventsTopic, ioConfig, "insert")
		Expect(test).To(BeTrue())

		mc := ioConfig.MongoConfig
		c := &mongo.Collection{
			Connection:   mc.Connection,
			Name:         mc.MetaCollectionName,
			Database:     mc.MetaDatabaseName,
			SchemaStruct: &AggregateMeta{},
		}

		collection, err := mongo.EnsureCollection(c)
		result, err := collection.FindOne(&AggregateMeta{
			AggregateID: 113,
		})
		Expect(err).ToNot(HaveOccurred())
		meta, assertOK := result.(*AggregateMeta)
		Expect(assertOK).To(BeTrue())
		Expect(meta.AggregateID).To(Equal(int8(113)))
		Expect(meta.Version).ToNot(Equal(int64(0)))
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
