package poll

import (
	"context"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// ReadConfig allows choosing what type of events should be processed.
// Warning: Be sure to read the associated EventsIO channel if it is enabled here.
// Otherwise it will result in a deadlock!
type ReadConfig struct {
	EnableDelete bool
	EnableInsert bool
	EnableQuery  bool
	EnableUpdate bool
}

// KafkaConfig is the configuration for Kafka, such as brokers and topics.
type KafkaConfig struct {
	// Consumer for EventStoreQuery-response
	ESQueryResCons *kafka.ConsumerConfig
	// Consumer for Event
	EventCons *kafka.ConsumerConfig

	// Producer for making requests to ESQuery
	ESQueryReqProd *kafka.ProducerConfig
	// Service-response Producer
	SvcResponseProd *kafka.ProducerConfig

	// Topic on which requests to EventStoreQuery should be sent.
	ESQueryReqTopic string
	// Topic on which the service should produce its response/results,
	// for use by other services.
	SvcResponseTopic string
}

// MongoConfig is the configuration for MongoDB client.
type MongoConfig struct {
	AggregateID   int8
	AggCollection *mongo.Collection
	// Collection/Database in which Aggregate metadata is stored.
	Connection         *mongo.ConnectionConfig
	MetaDatabaseName   string
	MetaCollectionName string
}

// IOConfig is the configuration for EventPoll service.
type IOConfig struct {
	KafkaConfig KafkaConfig
	MongoConfig MongoConfig
	ReadConfig  ReadConfig
}

// validateConfig validates the input config.
func validateConfig(config IOConfig) error {
	mc := config.MongoConfig
	if mc.AggregateID < 1 {
		return errors.New(
			"MongoConfig: AggregateID >0 is required, but none/invalid was specified",
		)
	}
	if mc.AggCollection == nil {
		return errors.New(
			"MongoConfig: AggCollection is required, but none was specified",
		)
	}
	if mc.Connection == nil {
		return errors.New(
			"MongoConfig: Connection is required, but none was specified",
		)
	}
	if mc.MetaDatabaseName == "" {
		return errors.New(
			"MongoConfig: MetaDatabaseName is required, but none was specified",
		)
	}
	if mc.MetaCollectionName == "" {
		return errors.New(
			"MongoConfig: MetaCollectionName is required, but none was specified",
		)
	}

	kc := config.KafkaConfig
	if kc.ESQueryResCons == nil {
		return errors.New(
			"KafkaConfig: ESQueryResCons is required, but none was specified",
		)
	}
	if kc.EventCons == nil {
		return errors.New(
			"KafkaConfig: EventCons is required, but none was specified",
		)
	}
	if kc.ESQueryReqProd == nil {
		return errors.New(
			"KafkaConfig: ESQueryReqProd is required, but none was specified",
		)
	}
	if kc.SvcResponseProd == nil {
		return errors.New(
			"KafkaConfig: SvcResponseProd is required, but none was specified",
		)
	}
	if kc.ESQueryReqTopic == "" {
		return errors.New(
			"KafkaConfig: ESQueryReqTopic is required, but none was specified",
		)
	}
	if kc.SvcResponseTopic == "" {
		return errors.New(
			"KafkaConfig: SvcResponseTopic is required, but none was specified",
		)
	}

	rc := config.ReadConfig
	if !rc.EnableDelete &&
		!rc.EnableInsert &&
		!rc.EnableQuery &&
		!rc.EnableUpdate {
		return errors.New(
			"ReadConfig: Atleast one of the channels must be open/true",
		)
	}

	return nil
}

// Init initializes the EventPoll service.
func Init(config IOConfig) (*EventsIO, error) {
	log.Println("Initializing Poll Service")

	err := validateConfig(config)
	if err != nil {
		err = errors.Wrap(err, "Error validating Configuration")
		return nil, err
	}

	krChan := make(chan *model.KafkaResponse)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	// closeChan will close all components of EventsPoll when anything is sent to it.
	closeChan := make(chan struct{})
	g.Go(func() error {
		<-closeChan
		log.Println("Received Close signal")
		close(krChan)
		log.Println("Signalling routines to close")
		cancel()
		close(closeChan)
		return nil
	})

	eventsIO := newEventsIO(ctx, g, closeChan, krChan)

	mgConfig := config.MongoConfig
	metaCollection, err := createMetaCollection(
		mgConfig.AggregateID,
		mgConfig.Connection,
		mgConfig.MetaDatabaseName,
		mgConfig.MetaCollectionName,
	)
	if err != nil {
		closeChan <- struct{}{}
		err = errors.Wrap(err, "Error initializing Meta-Collection")
		return nil, err
	}

	kfConfig := config.KafkaConfig

	// ESQueryRequest-Producer
	log.Println("Initializing ESQueryRequest-Producer")
	eventRespChan := make(chan model.KafkaResponse)
	err = esQueryReqProducer(&esQueryReqProdConfig{
		g:        g,
		closeCtx: ctx,

		aggID:           mgConfig.AggregateID,
		kafkaProdConfig: kfConfig.ESQueryReqProd,
		kafkaTopic:      kfConfig.ESQueryReqTopic,
		eventRespChan:   (<-chan model.KafkaResponse)(eventRespChan),
		mongoColl:       metaCollection,
	})
	if err != nil {
		closeChan <- struct{}{}
		return nil, err
	}

	// ESQueryResponse-Consumer
	log.Println("Initializing ESQueryResponse-Consumer")
	esRespConsumer, err := kafka.NewConsumer(kfConfig.ESQueryResCons)
	if err != nil {
		err = errors.Wrap(err, "Error creating ESQueryResponse-Consumer")
		closeChan <- struct{}{}
		return nil, err
	}
	g.Go(func() error {
		var consErr error
	errLoop:
		for {
			select {
			case <-ctx.Done():
				break errLoop
			case err := <-esRespConsumer.Errors():
				if err != nil {
					err = errors.Wrap(err, "Error in ESQueryResponse-Consumer")
					log.Println(err)
					consErr = err
					break errLoop
				}
			}
		}
		log.Println("--> Closed ESQueryResponse-Consumer error-routine")
		return consErr
	})

	// Result Producer
	log.Println("Initializing Result-Producer")
	err = resultProducer(&resultProducerConfig{
		g:          g,
		closeCtx:   ctx,
		resultChan: (<-chan *model.KafkaResponse)(krChan),
		prodConfig: kfConfig.SvcResponseProd,
		prodTopic:  kfConfig.SvcResponseTopic,
	})
	if err != nil {
		closeChan <- struct{}{}
		return nil, err
	}

	// Event-Consumer
	log.Println("Initializing Event-Consumer")
	eventConsumer, err := kafka.NewConsumer(kfConfig.EventCons)
	if err != nil {
		err = errors.Wrap(err, "Error creating Event-Consumer")
		closeChan <- struct{}{}
		return nil, err
	}
	g.Go(func() error {
		var consErr error
	errLoop:
		for {
			select {
			case <-ctx.Done():
				break errLoop
			case err := <-eventConsumer.Errors():
				if err != nil {
					err = errors.Wrap(err, "Error in Event-Consumer")
					log.Println(err)
					consErr = err
					break errLoop
				}
			}
		}
		log.Println("--> Closed Event-Consumer error-routine")
		return consErr
	})

	log.Println("Starting Consumers")

	// Event-Consumer Messages
	g.Go(func() error {
		handler := &eventHandler{
			eventRespChan: (chan<- model.KafkaResponse)(eventRespChan),
		}
		err := eventConsumer.Consume(ctx, handler)
		if err != nil {
			err = errors.Wrap(err, "Failed to consume Events")
		}
		log.Println("--> Closed Event-Consumer")
		return err
	})

	// ESQueryResponse-Consumer Messages
	g.Go(func() error {
		handler := &esRespHandler{
			aggID:          config.MongoConfig.AggregateID,
			eventsIO:       eventsIO,
			readConfig:     config.ReadConfig,
			metaCollection: metaCollection,
			versionChan:    make(chan int64, 128),
		}
		err = esRespConsumer.Consume(ctx, handler)
		if err != nil {
			err = errors.Wrap(err, "Failed to consume ESQueryResponse")
		}
		log.Println("--> Closed ESQueryResponse-Consumer")
		return err
	})

	// Drain svcResponse-channel
	g.Go(func() error {
		<-ctx.Done()
		for kr := range eventRespChan {
			log.Printf("EventResponse: Drained response with ID: %s", kr.UUID)
		}
		log.Println("--> Closed EventResponse drain-routine")
		return nil
	})

	// Drain ResultProducer-channel
	g.Go(func() error {
		<-ctx.Done()
		for kr := range krChan {
			log.Printf("svcResult: Drained result with ID: %s", kr.UUID)
		}
		log.Println("--> Closed svcResult drain-routine")
		return nil
	})

	// Drain io-channels
	rc := config.ReadConfig
	if rc.EnableDelete {
		g.Go(func() error {
			<-ctx.Done()
			for e := range eventsIO.delete {
				log.Printf("Delete: Drained event with ID: %s", e.Event.TimeUUID)
			}
			log.Println("--> Closed Delete drain-routine")
			return nil
		})
	}
	if rc.EnableInsert {
		g.Go(func() error {
			<-ctx.Done()
			for e := range eventsIO.insert {
				log.Printf("Insert: Drained event with ID: %s", e.Event.TimeUUID)
			}
			log.Println("--> Closed Insert drain-routine")
			return nil
		})
	}
	if rc.EnableQuery {
		g.Go(func() error {
			<-ctx.Done()
			for e := range eventsIO.query {
				log.Printf("Query: Drained event with ID: %s", e.Event.TimeUUID)
			}
			log.Println("--> Closed Query drain-routine")
			return nil
		})
	}
	if rc.EnableUpdate {
		g.Go(func() error {
			<-ctx.Done()
			for e := range eventsIO.update {
				log.Printf("Update: Drained event with ID: %s", e.Event.TimeUUID)
			}
			log.Println("--> Closed Update drain-routine")
			return nil
		})
	}

	log.Println("Events-Poll Service Initialized")
	return eventsIO, nil
}
