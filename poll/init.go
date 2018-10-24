package poll

import (
	ctx "context"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
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
	Brokers []string
	// Consumer Group-name for Events consumer.
	ConsumerEventGroup string
	// Consumer Group-name for EventStoreQuery response-consumer.
	ConsumerEventQueryGroup string
	// Topic on which responses from EventStoreQuery should be received.
	ConsumerEventQueryTopic string
	// Topic on which new events should be listened for.
	ConsumerEventTopic string
	// Topic on which requests to EventStoreQuery should be sent.
	ProducerEventQueryTopic string
	// Topic on which the service should produce its response/results,
	// for use by other services.
	ProducerResponseTopic string
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

	kfConfig := config.KafkaConfig
	if kfConfig.ConsumerEventGroup == "" {
		return errors.New(
			"KafkaConfig: ConsumerEventGroup is required, but none was specified",
		)
	}
	if kfConfig.ConsumerEventQueryGroup == "" {
		return errors.New(
			"KafkaConfig: ConsumerEventQueryGroup is required, but none was specified",
		)
	}
	if kfConfig.ConsumerEventQueryTopic == "" {
		return errors.New(
			"KafkaConfig: ConsumerEventQueryTopic is required, but none was specified",
		)
	}
	if kfConfig.ConsumerEventTopic == "" {
		return errors.New(
			"KafkaConfig: ConsumerEventTopic is required, but none was specified",
		)
	}
	if kfConfig.ProducerEventQueryTopic == "" {
		return errors.New(
			"KafkaConfig: ProducerEventQueryTopic is required, but none was specified",
		)
	}
	if kfConfig.ProducerResponseTopic == "" {
		return errors.New(
			"KafkaConfig: ProducerResponseTopic is required, but none was specified",
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

	cancelCtx, cancel := ctx.WithCancel(ctx.Background())
	krChan := make(chan *model.KafkaResponse)
	eventsIO := &EventsIO{
		cancelCtx:   &cancelCtx,
		cancelFunc:  &cancel,
		resultInput: krChan,
		delete:      make(chan *EventResponse),
		insert:      make(chan *EventResponse),
		query:       make(chan *EventResponse),
		update:      make(chan *EventResponse),
	}

	kfConfig := config.KafkaConfig
	mgConfig := config.MongoConfig

	// Events Consumer
	log.Println("Initializing Events Consumer")
	eventConsConf := &kafka.ConsumerConfig{
		GroupName:    kfConfig.ConsumerEventGroup,
		KafkaBrokers: kfConfig.Brokers,
		Topics:       []string{kfConfig.ConsumerEventTopic},
	}
	eventConsumer, err := kafka.NewConsumer(eventConsConf)
	if err != nil {
		cancel()
		return nil, err
	}

	prodConf := kafka.ProducerConfig{
		KafkaBrokers: kfConfig.Brokers,
	}

	metaCollection, err := createMetaCollection(
		mgConfig.AggregateID,
		mgConfig.Connection,
		mgConfig.MetaDatabaseName,
		mgConfig.MetaCollectionName,
	)
	if err != nil {
		cancel()
		err = errors.Wrap(err, "Error initializing Meta-Collection")
		return nil, err
	}

	// EventStoreQuery Request Producer
	eventRespChan := make(chan model.KafkaResponse)
	log.Println("Initializing EventStoreQuery Request Producer")
	esqReqProdConfig := &esQueryReqProdConfig{
		aggID:           mgConfig.AggregateID,
		cancelCtx:       &cancelCtx,
		mongoColl:       metaCollection,
		kafkaProdConfig: prodConf,
		eventRespChan:   eventRespChan,
		kafkaTopic:      kfConfig.ProducerEventQueryTopic,
	}
	err = esQueryReqProducer(esqReqProdConfig)
	if err != nil {
		cancel()
		return nil, err
	}

	// EventStoreQuery Response Consumer
	log.Println("Initializing EventStoreQuery Response Consumer")
	esRespConf := &kafka.ConsumerConfig{
		GroupName:    kfConfig.ConsumerEventQueryGroup,
		KafkaBrokers: kfConfig.Brokers,
		Topics:       []string{kfConfig.ConsumerEventQueryTopic},
	}
	esRespConsumer, err := kafka.NewConsumer(esRespConf)
	if err != nil {
		cancel()
		return nil, err
	}

	// Result Producer
	log.Println("Initializing Result Producer")

	err = resultProducer(
		&cancelCtx,
		prodConf,
		kfConfig.ProducerResponseTopic,
		(<-chan *model.KafkaResponse)(krChan),
	)
	if err != nil {
		cancel()
		return nil, err
	}

	log.Println("Starting Consumers")

	// Close Consumers as per CancelContext
	go func() {
		<-cancelCtx.Done()
		err := eventConsumer.Close()
		if err != nil {
			err = errors.Wrap(err, "Error closing EventConsumer")
		}
		log.Println("--> Closed EventConsumer")
		err = esRespConsumer.Close()
		if err != nil {
			err = errors.Wrap(err, "Error closing ESResponseConsumer")
		}
		log.Println("--> Closed ESResponseConsumer")
	}()

	// Event Consumer
	go func() {
		handler := &eventHandler{eventRespChan}
		err = eventConsumer.Consume(cancelCtx, handler)
		if err != nil {
			cancel()
			err = errors.Wrap(err, "Failed to consume Events")
			log.Fatalln(err)
		}
	}()

	// ESQuery Response Consumer
	go func() {
		handler := &esRespHandler{
			aggID:          config.MongoConfig.AggregateID,
			eventsIO:       eventsIO,
			readConfig:     config.ReadConfig,
			metaCollection: metaCollection,
		}
		err = esRespConsumer.Consume(cancelCtx, handler)
		if err != nil {
			cancel()
			err = errors.Wrap(err, "Failed to consume EventStoreQuery-Response")
			log.Fatalln(err)
		}
	}()

	log.Println("Events-Poll Service Initialized")
	return eventsIO, nil
}
