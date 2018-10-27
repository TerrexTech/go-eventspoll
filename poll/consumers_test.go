package poll

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync"
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

		cEventGroup := os.Getenv("KAFKA_CONSUMER_EVENT_GROUP")
		cESQueryGroup := os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_GROUP")
		cEventTopic := os.Getenv("KAFKA_CONSUMER_EVENT_TOPIC")
		cESQueryTopic := os.Getenv("KAFKA_CONSUMER_EVENT_QUERY_TOPIC")
		pESQueryTopic := os.Getenv("KAFKA_PRODUCER_EVENT_QUERY_TOPIC")
		pResponseTopic := os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		kc := KafkaConfig{
			EventCons: &kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    cEventGroup,
				Topics:       []string{cEventTopic},
			},
			ESQueryResCons: &kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    cESQueryGroup,
				Topics:       []string{cESQueryTopic},
			},

			ESQueryReqProd: &kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			},
			SvcResponseProd: &kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			},
			ESQueryReqTopic:  pESQueryTopic,
			SvcResponseTopic: pResponseTopic,
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

	Context("Events are produced", func() {
		Specify("Events should appear on their respective channel", func() {
			deleteEvent := mockEvent(eventProdInput, eventsTopic, "delete")
			insertEvent := mockEvent(eventProdInput, eventsTopic, "insert")
			updateEvent := mockEvent(eventProdInput, eventsTopic, "update")

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
			var insertLock sync.RWMutex

			updateSuccess := false
			var updateLock sync.RWMutex

			deleteSuccess := false
			var deleteLock sync.RWMutex

			g := eventsIO.RoutinesGroup()
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
							log.Println("==> A matching Event appeared on insert channel")
							insertLock.Lock()
							insertSuccess = true
							insertLock.Unlock()
							return nil
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
							updateLock.Lock()
							updateSuccess = true
							updateLock.Unlock()
							return nil
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
							deleteLock.Lock()
							deleteSuccess = true
							deleteLock.Unlock()
							return nil
						}
					}
				}
			})

			ds := false
			is := false
			us := false

		resultTimeoutLoop:
			for {
				select {
				case <-ctx.Done():
					break resultTimeoutLoop
				default:
					deleteLock.RLock()
					ds = deleteSuccess
					deleteLock.RUnlock()

					insertLock.RLock()
					is = insertSuccess
					insertLock.RUnlock()

					updateLock.RLock()
					us = updateSuccess
					updateLock.RUnlock()

					if ds && is && us {
						break resultTimeoutLoop
					}
				}
			}

			eventsIO.Close()
			<-eventsIO.Wait()

			Expect(is).To(BeTrue())
			Expect(us).To(BeTrue())
			Expect(ds).To(BeTrue())
		})
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

	Context("test response-generation", func() {
		It("should produce the response", func(done Done) {
			// Init Poll Service
			kfConfig := ioConfig.KafkaConfig
			consCfg := &kafka.ConsumerConfig{
				GroupName:    "poll-test-group",
				KafkaBrokers: kafkaBrokers,
				Topics:       []string{kfConfig.SvcResponseTopic},
			}
			respConsumer, err := kafka.NewConsumer(consCfg)
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

			eventsIO, err := Init(ioConfig)
			Expect(err).ToNot(HaveOccurred())

			// Here we again test that we only get responses on channels we have enabled
			mockEvent(eventProdInput, eventsTopic, "insert")
			mockEvent(eventProdInput, eventsTopic, "update")
			mockEvent(eventProdInput, eventsTopic, "delete")

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
			<-eventsIO.Wait()

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

	It("should execute cancel-context when the service is closed", func(done Done) {
		kc := ioConfig.KafkaConfig
		// Change ConsumerGroup so it doesn't interfere with other tests' groups
		kc.EventCons.GroupName = "test-e-group-close-2"
		kc.ESQueryResCons.GroupName = "test-eq-group-close-2"

		ioConfig.ReadConfig = ReadConfig{
			EnableDelete: false,
			EnableInsert: true,
			EnableQuery:  false,
			EnableUpdate: false,
		}

		eventsIO, err := Init(ioConfig)
		Expect(err).ToNot(HaveOccurred())

		eventsIO.Close()
		<-eventsIO.Wait()
		close(done)
	}, 15)
})
