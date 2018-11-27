package poll

import (
	"context"
	"log"

	"github.com/TerrexTech/go-common-models/model"
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
	// Topic on which requests to EventStoreQuery should be sent.
	ESQueryReqTopic string
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
}

// validateConfig validates the input config.
func validateConfig(config IOConfig) error {
	mc := config.MongoConfig
	if mc.AggregateID < 1 {
		return errors.New(
			"MongoConfig: AggregateID >0 is required, but none/invalid was specified",
		)
	}
	if mc.AggCollection == nil || mc.AggCollection.Connection == nil {
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
	if kc.ESQueryReqTopic == "" {
		return errors.New(
			"KafkaConfig: ESQueryReqTopic is required, but none was specified",
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

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	// closeChan will close all components of EventsPoll when anything is sent to it.
	closeChan := make(chan struct{})

	eventsIO := newEventsIO(ctx, g, closeChan)
	g.Go(func() error {
		<-closeChan
		eventsIO.ctxLock.Lock()
		eventsIO.ctxOpen = false
		eventsIO.ctxLock.Unlock()

		log.Println("Received Close signal")
		log.Println("Signalling routines to close")
		cancel()
		close(closeChan)
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		eventsIO.Close()
		return nil
	})

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
	eventRespChan := make(chan model.Document)
	err = esQueryReqProducer(&esQueryReqProdConfig{
		g:        g,
		closeCtx: ctx,

		aggID:           mgConfig.AggregateID,
		kafkaProdConfig: kfConfig.ESQueryReqProd,
		kafkaTopic:      kfConfig.ESQueryReqTopic,
		mongoColl:       metaCollection,

		eventRespTopic: kfConfig.ESQueryResCons.Topics[0],
		eventRespChan:  (<-chan model.Document)(eventRespChan),
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
			eventRespChan: (chan<- model.Document)(eventRespChan),
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
		for doc := range eventRespChan {
			log.Printf("EventResponse: Drained response with ID: %s", doc.UUID)
		}
		log.Println("--> Closed EventResponse drain-routine")
		return nil
	})

	// Drain even-channel
	g.Go(func() error {
		<-ctx.Done()
		for e := range eventsIO.eventResp {
			log.Printf("EventResp: Drained event with ID: %s", e.Event.UUID)
		}
		log.Println("--> Closed EventResp drain-routine")
		return nil
	})
	log.Println("Events-Poll Service Initialized")
	return eventsIO, nil
}
